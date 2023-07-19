use async_graphql::{Context, Object, SimpleObject, Schema, EmptySubscription, InputObject};
use lapin::{options::BasicPublishOptions, BasicProperties};
use pmtree::Hasher;
use rln::circuit::Fr;
use secp256k1::{Secp256k1, Message, ecdsa, PublicKey};
use serde_with::serde_as;
use sha3::{Digest, Keccak256};
use serde::{Serialize, Deserialize};

use crate::{merkle::MyPoseidon, ApiContext};
pub(crate) type ServiceSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

pub(crate) struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Returns the merkle root.
    /// Unsafe because it puts a lock on the merkle tree.
    async fn unsafe_root(&self, ctx: &Context<'_>) -> Vec<u8> {
        let mt = ctx.data_unchecked::<ApiContext>().mt.read().await;
        MyPoseidon::serialize(mt.root())
    }

    async fn initiate_send(&self, ctx: &Context<'_>, address: [u8; 20], amount: u64) -> bool {
        dbg!(&amount);
        let db = &ctx.data_unchecked::<ApiContext>().user_balance_db;

        db.is_balance_sufficient(&address, &amount.to_be_bytes()).await.unwrap()
    }
}

pub(crate) struct MutationRoot;

/// Leaf structure of the merkle tree.
/// `address` owns the coin range `[low_coin, high_coin]`.
#[derive(InputObject, Serialize, Deserialize)]
struct Leaf {
    address: [u8; 20],
    low_coin: u64,
    high_coin: u64,
}

#[derive(SimpleObject, Serialize, Deserialize)]
struct MerkleInfo {
    // See https://docs.rs/serde_bytes/latest/serde_bytes for this tag.
    #[serde(with = "serde_bytes")]
    root: Vec<u8>,
    #[serde(with = "serde_bytes")]
    leaf: Vec<u8>,
}

// `serde` crate can't serialize large arrays, hence using specially designed `serde_with`.
#[serde_as]
#[derive(InputObject, Serialize, Deserialize)]
struct Signature {
    #[serde_as(as = "[_; 64]")]
    sig: [u8; 64],
    #[serde_as(as = "[_; 65]")]
    pubkey: [u8; 65],
}

#[Object]
impl MutationRoot {
    /// Insert a leaf in the merkle tree.
    async fn unsafe_insert(&self, ctx: &Context<'_>, leaf: Leaf) -> MerkleInfo {
        let mut mt = ctx.data_unchecked::<ApiContext>().mt.write().await;
        let mut address = vec![0u8; 12];
        address.extend_from_slice(&leaf.address);
        let hash = MyPoseidon::hash(&[MyPoseidon::deserialize(address), Fr::from(leaf.low_coin), Fr::from(leaf.high_coin)]);
        mt.update_next(hash).await.unwrap();
        let leaf_index = mt.leaves_set() - 1;
        MerkleInfo {
            root: MyPoseidon::serialize(mt.root()),
            leaf: MyPoseidon::serialize(mt.get(leaf_index).await.unwrap())
        }
    }

    /// Send coins `[leaf.low_coin, highest_coin_to_send]` to `receiver` from `leaf`.
    /// The send should be authorized by `leaf.address` through ECDSA signature `sig`.
    async fn unsafe_send(
        &self,
        ctx: &Context<'_>,
        leaf: Leaf,
        key: usize,
        highest_coin_to_send: u64,
        recipient: Vec<u8>,
        sig: Signature,
    ) -> Vec<u8> {
        let api_context = ctx.data_unchecked::<ApiContext>();
        let mut mt = api_context.mt.write().await;
        let hashed_leaf = mt.get(key).await.unwrap();

        let mut sender = vec![0u8; 12];
        sender.extend_from_slice(&leaf.address);
        assert_eq!(hashed_leaf, MyPoseidon::hash(&[MyPoseidon::deserialize(sender.clone()), Fr::from(leaf.low_coin), Fr::from(leaf.high_coin)]));

        let mut receiver = vec![0u8; 12];
        receiver.extend_from_slice(&recipient);
        let msg = MyPoseidon::hash(&[Fr::from(leaf.low_coin), Fr::from(leaf.high_coin), Fr::from(highest_coin_to_send), MyPoseidon::deserialize(receiver.clone())]);
        dbg!(MyPoseidon::serialize(msg));

        let secp = Secp256k1::verification_only();
        assert!(secp.verify_ecdsa(
            &Message::from_slice(&MyPoseidon::serialize(msg)).unwrap(),
            &ecdsa::Signature::from_compact(&sig.sig).unwrap(),
            &PublicKey::from_slice(&sig.pubkey).unwrap(),
        ).is_ok());

        assert_eq!(Keccak256::digest(&sig.pubkey[1..65]).as_slice()[12..], leaf.address, "address doesn't match");

        mt.set(key, MyPoseidon::hash(&[MyPoseidon::deserialize(sender), Fr::from(highest_coin_to_send+1), Fr::from(leaf.high_coin)])).await.unwrap();
        mt.update_next(MyPoseidon::hash(&[MyPoseidon::deserialize(receiver), Fr::from(leaf.low_coin), Fr::from(highest_coin_to_send)])).await.unwrap();

        let channel = &api_context.channel;

        // Serialize variables into Vec<u8>
        let queue_message = bincode::serialize(&(leaf, key, highest_coin_to_send, recipient, sig))
            .expect("unsafe_send: queue message should be serialized");

        let confirm = channel
            .basic_publish(
                "",
                "send_request_queue",
                BasicPublishOptions {
                    mandatory: true,
                    ..BasicPublishOptions::default()
                },
                queue_message.as_slice(),
                BasicProperties::default(),
            )
            .await.expect("basic_publish").await.expect("publisher-confirms");

        assert!(confirm.is_ack());
        // when `mandatory` is on, if the message is not sent to a queue for any reason
        // (example, queues are full), the message is returned back.
        // If the message isn't received back, then a queue has received the message.
        assert_eq!(confirm.take_message(), None);

        MyPoseidon::serialize(mt.root())
    }
}
