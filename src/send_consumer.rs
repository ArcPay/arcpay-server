use futures_lite::stream::StreamExt;
use lapin::{options::BasicAckOptions, Consumer};
use pmtree::Hasher;
use rln::circuit::Fr;
use secp256k1::{ecdsa, Message, PublicKey, Secp256k1};
use sha3::{Digest, Keccak256};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_postgres::NoTls;

use crate::QueueMessage;
use crate::{
    merkle::{MyPoseidon, PostgresDBConfig},
    model::Leaf,
    model::Signature,
};

/// Verify signature and public key in `sig` is correct.
pub(crate) fn verify_ecdsa(
    leaf: &Leaf,
    highest_coin_to_send: u64,
    receiver: &[u8],
    sig: &Signature,
) {
    let msg = MyPoseidon::hash(&[
        Fr::from(leaf.low_coin),
        Fr::from(leaf.high_coin),
        Fr::from(highest_coin_to_send),
        MyPoseidon::deserialize(receiver.to_owned()),
    ]);

    // Verify signature is correct.
    let secp = Secp256k1::verification_only();
    assert!(secp
        .verify_ecdsa(
            &Message::from_slice(&MyPoseidon::serialize(msg)).unwrap(),
            &ecdsa::Signature::from_compact(&sig.sig).unwrap(),
            &PublicKey::from_slice(&sig.pubkey).unwrap(),
        )
        .is_ok());

    assert_eq!(
        Keccak256::digest(&sig.pubkey[1..65]).as_slice()[12..],
        leaf.address,
        "address doesn't match"
    );
}

// Run this in a separate thread.
pub(crate) async fn send_consumer(mut consumer: Consumer) {
    /*
    let (client, connection) =
        tokio_postgres::connect("host=localhost user=dev dbname=prover", NoTls)
            .await
            .unwrap();

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    let client = Arc::new(RwLock::new(client));
    let db_config = PostgresDBConfig {
        client: client.clone(),
        merkle_table: "test".to_string(),
        pre_image_table: "pre_image".to_string(),
    };

    let mt = match cli {
        MerkleCommand::New => {
            MerkleTree::<PostgresDBConfig, MyPoseidon>::new(MERKLE_DEPTH, db_config.clone())
                .await
                .unwrap()
        }
        MerkleCommand::Load => MerkleTree::<PostgresDBConfig, MyPoseidon>::load(db_config.clone())
            .await
            .unwrap(),
    };
     */

    // Process incoming messages in a separate thread.
    while let Some(delivery) = consumer.next().await {
        let delivery = delivery.expect("error in consumer");
        let mesg: QueueMessage = bincode::deserialize(delivery.data.as_slice())
            .expect("deserialization should be correct");

        match mesg {
            QueueMessage::Mint { receiver, amount } => {
                dbg!(receiver, amount);
            }
            QueueMessage::Send(send) => {
                let (
                    leaf,
                    index,
                    highest_coin_to_send,
                    recipient,
                    sig,
                    from_proof,
                    sender_new_proof,
                    recipient_new_proof,
                ) = send;
                let mut receiver = vec![0u8; 12];
                receiver.extend_from_slice(&recipient);
                verify_ecdsa(&leaf, highest_coin_to_send, &receiver, &sig);
                println!("verified ecdsa");
            }
        }

        delivery
            .ack(BasicAckOptions::default())
            .await
            .expect("basic_ack");
    }
}
