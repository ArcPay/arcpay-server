use std::sync::Arc;

use ethers::prelude::*;
use lapin::{options::BasicPublishOptions, BasicProperties, Channel};
use tokio::sync::RwLock;

use crate::{
    arc_pay_contract,
    merkle::{MyPoseidon, PostgresDBConfig},
    model::mint_in_merkle,
    QueueMessage, QUEUE_NAME,
};

pub(crate) async fn mint(
    events: Event<Arc<Provider<Http>>, Provider<Http>, arc_pay_contract::MintFilter>,
    channel: Arc<Channel>,
    mt: Arc<RwLock<pmtree::MerkleTree<PostgresDBConfig, MyPoseidon>>>,
) {
    let mut stream = events.stream().await.unwrap();
    while let Some(Ok(f)) = stream.next().await {
        // check what happens when amount overflows u64.
        let mut mt = mt.write().await;
        // TODO check what happens when amount overflows u64.
        mint_in_merkle(&mut mt, f.receiver.into(), f.amount.as_u64()).await;
        drop(mt);
        let queue_message = bincode::serialize(&QueueMessage::Mint {
            receiver: f.receiver,
            amount: f.amount,
        })
        .unwrap();
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
}
