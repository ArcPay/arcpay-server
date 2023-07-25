use std::sync::Arc;

use ethers::prelude::*;
use lapin::{options::BasicPublishOptions, BasicProperties, Channel};

use crate::{arc_pay_contract, QueueMessage, QUEUE_NAME};

pub(crate) async fn mint(
    events: Event<Arc<Provider<Http>>, Provider<Http>, arc_pay_contract::MintFilter>,
    channel: Arc<Channel>,
) {
    let mut stream = events.stream().await.unwrap();

    while let Some(Ok(f)) = stream.next().await {
        let queue_message = bincode::serialize(&QueueMessage::Mint {
            receiver: f.receiver,
            amount: f.amount,
        })
        .unwrap();
        dbg!(&queue_message);
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
}
