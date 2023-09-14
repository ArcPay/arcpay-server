use ethers::types::{Address, Transaction};
use tokio::sync::RwLockWriteGuard;

use async_graphql::{Context, EmptySubscription, InputObject, Object, Schema, SimpleObject};
use lapin::{options::BasicPublishOptions, BasicProperties};
use pmtree::{Hasher, MerkleTree};
use rln::circuit::Fr;
use serde::{Deserialize, Deserializer, Serialize};
use serde_with::serde_as;
use tokio_postgres::IsolationLevel;

use crate::{
    merkle::{MyPoseidon, PostgresDBConfig},
    send_consumer::verify_ecdsa,
    transactions::primitive::PrimitiveTransaction,
    transactions::{
        multicoin::{MultiCoinSend, SignedMultiCoinSend},
        RichTransaction,
    },
    ApiContext, QueueMessage, QUEUE_NAME,
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

    // TODO: add some authentication for user privacy, maybe a signature.
    async fn get_ownership_proofs(&self, ctx: &Context<'_>, address: [u8; 20]) -> Vec<CoinRange> {
        let merkle_db = &ctx.data_unchecked::<ApiContext>().mt.read().await.db;
        merkle_db.get_for_address(&address).await
    }

    async fn initiate_send(&self, ctx: &Context<'_>, address: [u8; 20], amount: u64) -> bool {
        let db = &ctx.data_unchecked::<ApiContext>().user_balance_db;

        db.is_balance_sufficient(&address, &amount.to_be_bytes())
            .await
            .unwrap()
    }
}

pub(crate) struct MutationRoot;

/// Leaf structure of the merkle tree.
/// `address` owns the coin range `[low_coin, high_coin]`.
#[derive(Debug, Clone, InputObject, Serialize, Deserialize)]
pub(crate) struct Leaf {
    pub address: [u8; 20],
    pub low_coin: u64,
    pub high_coin: u64,
}

#[derive(Debug, Serialize, SimpleObject)]
pub(crate) struct CoinRange {
    pub index: usize,
    pub low_coin: u64,
    pub high_coin: u64,
}

// `serde` crate can't serialize large arrays, hence using specially designed `serde_with`.
#[serde_as]
#[derive(Debug, InputObject, Serialize, Deserialize, Clone)]
pub(crate) struct Signature {
    #[serde_as(as = "[_; 32]")]
    pub r: [u8; 32],
    #[serde_as(as = "[_; 32]")]
    pub s: [u8; 32],
    pub v: u64,
}

impl From<Signature> for ethers::prelude::Signature {
    fn from(sig: Signature) -> Self {
        ethers::prelude::Signature {
            r: sig.r.into(),
            s: sig.s.into(),
            v: sig.v,
        }
    }
}

#[derive(Debug, SimpleObject, Serialize, Deserialize)]
pub(crate) struct MerkleInfo {
    // See https://docs.rs/serde_bytes/latest/serde_bytes for this tag.
    #[serde(with = "serde_bytes")]
    root: Vec<u8>,
    #[serde(with = "serde_bytes")]
    leaf: Vec<u8>,
}

#[derive(Debug)]
pub(crate) struct MyFr(Fr);

pub(crate) type MerkleProof = Vec<(MyFr, u8)>;
pub(crate) type SendMessageType = (Leaf, usize, u64, [u8; 20], [MerkleProof; 3]);
pub(crate) type WithdrawMessageType = (Leaf, usize);

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

pub(crate) async fn mint_in_merkle(
    mt: &mut RwLockWriteGuard<'_, MerkleTree<PostgresDBConfig, MyPoseidon>>,
    leaf: Leaf,
    tx: &tokio_postgres::Transaction<'_>,
) -> MerkleInfo {
    assert!(leaf.low_coin <= leaf.high_coin);

    let hash = MyPoseidon::hash(&[
        MyPoseidon::deserialize(leaf.address.into()),
        Fr::from(leaf.low_coin),
        Fr::from(leaf.high_coin),
    ]);
    mt.update_next(hash, Some(leaf), tx).await.unwrap();
    MerkleInfo {
        root: MyPoseidon::serialize(mt.root()),
        leaf: MyPoseidon::serialize(hash),
    }
}

pub(crate) async fn send_in_merkle(
    mt: &mut RwLockWriteGuard<'_, MerkleTree<PostgresDBConfig, MyPoseidon>>,
    index: u64,
    leaf: &Leaf,
    highest_coin_to_send: u64,
    recipient: &[u8; 20],
    is_return_proof: bool,
    tx: &tokio_postgres::Transaction<'_>,
) -> Option<[Vec<(MyFr, u8)>; 3]> {
    assert!(
        leaf.low_coin <= highest_coin_to_send && highest_coin_to_send <= leaf.high_coin,
        "highest_coin_to_send should be in leaf range"
    );

    let mut proofs: [Vec<(MyFr, u8)>; 3] = [vec![], vec![], vec![]];

    ///// Verify signature and public key in `sig` is correct. /////
    let mut receiver = vec![0u8; 12];
    receiver.extend_from_slice(recipient);

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

    if is_return_proof {
        let from_proof = to_my_fr(mt.proof(index as usize).await.unwrap().0);
        proofs[0] = from_proof;
    }
    match highest_coin_to_send == leaf.high_coin {
        true => mt.set(index as usize, MyPoseidon::default_leaf(), None, tx),
        false => mt.set(
            index as usize,
            MyPoseidon::hash(&[
                MyPoseidon::deserialize(sender),
                Fr::from(highest_coin_to_send + 1), // invariant: highest_coin_to_send+1 <= leaf.high_coin
                Fr::from(leaf.high_coin),
            ]),
            Some(Leaf {
                address: leaf.address,
                low_coin: highest_coin_to_send + 1,
                high_coin: leaf.high_coin,
            }),
            tx,
        ),
    }
    .await
    .unwrap();

    let receiver_index = mt.leaves_set();

    // Add a new leaf for recipient.
    mt.update_next(
        MyPoseidon::hash(&[
            MyPoseidon::deserialize(receiver),
            Fr::from(leaf.low_coin),
            Fr::from(highest_coin_to_send),
        ]),
        Some(Leaf {
            address: *recipient,
            low_coin: leaf.low_coin,
            high_coin: highest_coin_to_send,
        }),
        tx,
    )
    .await
    .unwrap();

    if is_return_proof {
        let sender_new_proof = to_my_fr(mt.proof(index as usize).await.unwrap().0);
        let recipient_new_proof = to_my_fr(mt.proof(receiver_index).await.unwrap().0);
        proofs[1] = sender_new_proof;
        proofs[2] = recipient_new_proof;
    }

    match is_return_proof {
        true => Some(proofs),
        false => None,
    }
}

#[Object]
impl MutationRoot {
    async fn multi_coin_send(
        &self,
        ctx: &Context<'_>,
        multi_coin_tx: SignedMultiCoinSend,
    ) -> Vec<u8> {
        let api_context = ctx.data_unchecked::<ApiContext>();
        let mut mt = api_context.mt.write().await;
        let client = mt.db.client.write().await;
        let tx = client
            .build_transaction()
            .isolation_level(IsolationLevel::Serializable)
            .start()
            .await
            .expect("Database::put_with_pre_image build_transaction error");

        multi_coin_tx.authorized().unwrap();

        let components = multi_coin_tx.decompose();

        let mut root = vec![];
        for primitive_tx in components.iter() {
            let index = primitive_tx.leaf_id();
            let leaf = Leaf {
                address: primitive_tx.sender(),
                low_coin: primitive_tx.low_coin(),
                high_coin: primitive_tx.high_coin(),
            };
            let highest_coin_to_send = primitive_tx.fee_upper_bound() - 1; // TODO: move everything to fee upper bound
            let recipient = primitive_tx.receiver();

            let proofs = send_in_merkle(
                &mut mt,
                index,
                &leaf,
                highest_coin_to_send,
                &recipient,
                true,
                &tx,
            )
            .await
            .ok_or("proofs should be returned")
            .unwrap();

            root = MyPoseidon::serialize(mt.root());

            // Queue the send request to be received by ZK prover at the other end.
            let channel = &api_context.channel;

            let queue_message = bincode::serialize(&QueueMessage::Send((
                leaf,
                index as usize,
                highest_coin_to_send,
                recipient,
                proofs,
            )))
            .expect("unsafe_send: queue message should be serialized");

            let client = api_context.channel.write().await;
            // .execute("INSERT INTO message_queue (payload) VALUES ($1)", &[&data]).await.unwrap();

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
        }

        root
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
        let api_context = ctx.data_unchecked::<ApiContext>();
        let mut mt = api_context.mt.write().await;

        verify_ecdsa(&leaf, highest_coin_to_send, &recipient, sig);
        let client = mt.db.client.write().await;
        let tx = client
            .build_transaction()
            .isolation_level(IsolationLevel::Serializable)
            .start()
            .await
            .expect("Database::put_with_pre_image build_transaction error");
        let proofs = send_in_merkle(
            &mut mt,
            index,
            &leaf,
            highest_coin_to_send,
            &recipient,
            true,
            &tx,
        )
        .await
        .ok_or("proofs should be returned")
        .unwrap();

        let root = MyPoseidon::serialize(mt.root());
        drop(mt);

        // Queue the send request to be received by ZK prover at the other end.
        let channel = &api_context.channel;

        let queue_message = bincode::serialize(&QueueMessage::Send((
            leaf,
            index as usize,
            highest_coin_to_send,
            recipient,
            proofs,
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

        root
    }

    async fn withdraw(&self, ctx: &Context<'_>, index: u64, leaf: Leaf, sig: Signature) -> Vec<u8> {
        let zero_addr = Address::default(); // TODO confirm
        verify_ecdsa(&leaf, leaf.high_coin, &zero_addr.into(), sig);

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

        let client = mt.db.client.write().await;

        let tx = client
            .build_transaction()
            .isolation_level(IsolationLevel::Serializable)
            .start()
            .await
            .expect("Database::put_with_pre_image build_transaction error");

        mt.set(index as usize, MyPoseidon::default_leaf(), None, &tx)
            .await
            .unwrap();

        // Queue the withdraw request to be received by ZK prover at the other end.
        let channel = &api_context.channel;

        let queue_message = bincode::serialize(&QueueMessage::Withdraw((leaf, index as usize)))
            .expect("withdraw: queue message should be serialized");

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
