use async_graphql::{Context, EmptySubscription, InputObject, Object, Schema, SimpleObject};
use lapin::{options::BasicPublishOptions, BasicProperties};
use pmtree::Hasher;
use rln::circuit::Fr;
use serde::{Deserialize, Deserializer, Serialize};
use serde_with::serde_as;

use crate::{
    merkle::MyPoseidon, send_consumer::verify_ecdsa, ApiContext, QueueMessage, MERKLE_DEPTH,
    QUEUE_NAME,
};
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

        db.is_balance_sufficient(&address, &amount.to_be_bytes())
            .await
            .unwrap()
    }
}

pub(crate) struct MutationRoot;

/// Leaf structure of the merkle tree.
/// `address` owns the coin range `[low_coin, high_coin]`.
#[derive(Debug, InputObject, Serialize, Deserialize)]
pub(crate) struct Leaf {
    pub address: [u8; 20],
    pub low_coin: u64,
    pub high_coin: u64,
}

// `serde` crate can't serialize large arrays, hence using specially designed `serde_with`.
#[serde_as]
#[derive(Debug, InputObject, Serialize, Deserialize)]
pub(crate) struct Signature {
    #[serde_as(as = "[_; 64]")]
    pub sig: [u8; 64],
    #[serde_as(as = "[_; 65]")]
    pub pubkey: [u8; 65],
}

#[derive(Debug, SimpleObject, Serialize, Deserialize)]
struct MerkleInfo {
    // See https://docs.rs/serde_bytes/latest/serde_bytes for this tag.
    #[serde(with = "serde_bytes")]
    root: Vec<u8>,
    #[serde(with = "serde_bytes")]
    leaf: Vec<u8>,
}

#[derive(Debug)]
pub(crate) struct MyFr(Fr);

pub(crate) type MerkleProof = Vec<(MyFr, u8)>;
pub(crate) type SendMessageType = (
    Leaf,
    usize,
    u64,
    [u8; 20],
    Signature,
    MerkleProof,
    MerkleProof,
    MerkleProof,
);

impl Serialize for MyFr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let serialized_data = MyPoseidon::serialize(self.0);
        serializer.serialize_bytes(&serialized_data)
    }
}

impl<'de> Deserialize<'de> for MyFr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Deserialize the binary data into a Vec<u8>
        let serialized_data: Vec<u8> = Vec::deserialize(deserializer)?;

        // Call your existing deserialize function to construct MyFr from Vec<u8>
        let my_fr_instance = MyFr(MyPoseidon::deserialize(serialized_data));

        // Return the newly constructed MyFr instance
        Ok(my_fr_instance)
    }
}

fn to_my_fr(from: Vec<(Fr, u8)>) -> Vec<(MyFr, u8)> {
    from.iter().map(|(fr, val)| (MyFr(*fr), *val)).collect()
}

#[Object]
impl MutationRoot {
    /// Insert a leaf in the merkle tree.
    async fn unsafe_insert(&self, ctx: &Context<'_>, leaf: Leaf) -> MerkleInfo {
        let mut mt = ctx.data_unchecked::<ApiContext>().mt.write().await;
        let hash = MyPoseidon::hash(&[
            MyPoseidon::deserialize(leaf.address.into()),
            Fr::from(leaf.low_coin),
            Fr::from(leaf.high_coin),
        ]);
        mt.update_next(hash, Some(leaf)).await.unwrap();
        let leaf_index = mt.leaves_set() - 1;
        MerkleInfo {
            root: MyPoseidon::serialize(mt.root()),
            leaf: MyPoseidon::serialize(mt.get(leaf_index).await.unwrap()),
        }
    }

    /// Send coins `[leaf.low_coin, highest_coin_to_send]` to `receiver` from `leaf`.
    /// The send should be authorized by `leaf.address` through ECDSA signature `sig`.
    async fn unsafe_send(
        &self,
        ctx: &Context<'_>,
        index: u64,
        leaf: Leaf,
        highest_coin_to_send: u64,
        recipient: [u8; 20],
        sig: Signature,
    ) -> Vec<u8> {
        ///// Verify signature and public key in `sig` is correct. /////
        let mut receiver = vec![0u8; 12];
        receiver.extend_from_slice(&recipient);
        verify_ecdsa(&leaf, highest_coin_to_send, &receiver, &sig);

        let api_context = ctx.data_unchecked::<ApiContext>();
        let mut mt = api_context.mt.write().await;

        let hashed_leaf = mt.get(index as usize).await.unwrap();

        // Verify that sender's leaf is the same as the hash of the input.
        let mut sender = vec![0u8; 12];
        sender.extend_from_slice(&leaf.address);
        assert_eq!(
            hashed_leaf,
            MyPoseidon::hash(&[
                MyPoseidon::deserialize(sender.clone()),
                Fr::from(leaf.low_coin),
                Fr::from(leaf.high_coin)
            ])
        );

        let from_proof = to_my_fr(mt.proof(index as usize).await.unwrap().0);

        mt.set(
            index as usize,
            MyPoseidon::hash(&[
                MyPoseidon::deserialize(sender),
                Fr::from(highest_coin_to_send + 1),
                Fr::from(leaf.high_coin),
            ]),
            Some(Leaf {
                address: leaf.address,
                low_coin: highest_coin_to_send + 1,
                high_coin: leaf.high_coin,
            }),
        )
        .await
        .unwrap();

        let sender_new_proof = to_my_fr(mt.proof(index as usize).await.unwrap().0);

        let receiver_index = mt.leaves_set();

        // Add a new leaf for recipient.
        mt.update_next(
            MyPoseidon::hash(&[
                MyPoseidon::deserialize(receiver),
                Fr::from(leaf.low_coin),
                Fr::from(highest_coin_to_send),
            ]),
            Some(Leaf {
                address: recipient,
                low_coin: leaf.low_coin,
                high_coin: highest_coin_to_send,
            }),
        )
        .await
        .unwrap();

        let recipient_new_proof = to_my_fr(mt.proof(receiver_index).await.unwrap().0);

        assert_eq!(from_proof.len(), MERKLE_DEPTH);

        // Queue the send request to be received by ZK prover at the other end.
        let channel = &api_context.channel;

        let queue_message = bincode::serialize(&QueueMessage::Send((
            leaf,
            index as usize,
            highest_coin_to_send,
            recipient,
            sig,
            from_proof,
            sender_new_proof,
            recipient_new_proof,
        )))
        .expect("unsafe_send: queue message should be serialized");

        let confirm = channel
            .basic_publish(
                "",
                QUEUE_NAME,
                BasicPublishOptions {
                    mandatory: true,
                    ..BasicPublishOptions::default()
                },
                queue_message.as_slice(),
                BasicProperties::default(),
            )
            .await
            .expect("basic_publish")
            .await
            .expect("publisher-confirms");

        assert!(confirm.is_ack());
        // when `mandatory` is on, if the message is not sent to a queue for any reason
        // (example, queues are full), the message is returned back.
        // If the message isn't received back, then a queue has received the message.
        assert_eq!(confirm.take_message(), None);

        MyPoseidon::serialize(mt.root())
    }
}
