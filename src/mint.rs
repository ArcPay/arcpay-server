use std::sync::Arc;

use ethers::prelude::*;
use lapin::{options::BasicPublishOptions, BasicProperties, Channel};
use tokio::sync::RwLock;

use crate::{
    arc_pay_contract,
    merkle::{MyPoseidon, PostgresDBConfig},
    model::{mint_in_merkle, Leaf},
    MintFilter, QueueMessage, QUEUE_NAME,
};

pub(crate) async fn mint(
    contract: arc_pay_contract::ArcPayContract<Provider<Http>>,
    channel: Arc<Channel>,
    mt: Arc<RwLock<pmtree::MerkleTree<PostgresDBConfig, MyPoseidon>>>,
) {
    dbg!("inmint");
    // see tracking past vs future events: https://github.com/gakonst/ethers-rs/issues/988
    let z = contract.event::<MintFilter>();
    let mut stream = z.stream().await.unwrap();
    dbg!("onmint");
    // see tracking past vs future events: https://github.com/gakonst/ethers-rs/issues/988

    while let Some(Ok(f)) = stream.next().await {
        dbg!(&f);
        // check what happens when amount overflows u64.
        let mut mt = mt.write().await;
        // TODO check what happens when amount overflows u64.
        let leaf = Leaf {
            address: f.receiver.into(),
            low_coin: f.low_coin.as_u64(),
            high_coin: f.high_coin.as_u64(),
        };
        mint_in_merkle(&mut mt, leaf.clone()).await;
        drop(mt);
        let queue_message = bincode::serialize(&QueueMessage::Mint((leaf, f.timestamp))).unwrap();
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

        assert!(confirm.is_ack(), "no confirmation on publisher");
        // when `mandatory` is on, if the message is not sent to a queue for any reason
        // (example, queues are full), the message is returned back.
        // If the message isn't received back, then a queue has received the message.
        assert_eq!(
            confirm.take_message(),
            None,
            "queue didn't receive the mint mesg"
        );
    }
    dbg!("finished");
}
