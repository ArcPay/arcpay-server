use futures_lite::stream::StreamExt;
use lapin::{options::BasicAckOptions, Consumer};
use pmtree::Hasher;
use rln::circuit::Fr;
use secp256k1::{ecdsa, Message, PublicKey, Secp256k1};
use sha3::{Digest, Keccak256};
use tokio::sync::RwLock;

use crate::{
    merkle::{MyPoseidon, PostgresDBConfig},
    model::{mint_in_merkle, Leaf},
    model::{send_in_merkle, Signature},
};
use crate::{QueueMessage, MAX_SINCE_LAST_PROVE};

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
pub(crate) async fn send_consumer(
    mut consumer: Consumer,
    mt: RwLock<pmtree::MerkleTree<PostgresDBConfig, MyPoseidon>>,
) {
    while let Some(delivery) = consumer.next().await {
        let delivery = delivery.expect("error in consumer");
        let mesg: QueueMessage = bincode::deserialize(delivery.data.as_slice())
            .expect("deserialization should be correct");

        let mut mt = mt.write().await;
        match mesg {
            QueueMessage::Mint { receiver, amount } => {
                dbg!(receiver, amount);
                // TODO check what happens when amount overflows u64.
                mint_in_merkle(&mut mt, receiver.into(), amount.as_u64()).await;
                // Send it to be proved (block current thread).
                // Persist proof.
                // Check now - last proof time > MAX_SINCE_LAST_PROOF.
                // If yes, prove the nova proof for groth16 and issue transaction.
                // If no, nothing.
            }
            QueueMessage::Send(send) => {
                let (leaf, index, highest_coin_to_send, recipient, sig, proofs) = send;
                send_in_merkle(
                    &mut mt,
                    index as u64,
                    &leaf,
                    highest_coin_to_send,
                    &recipient,
                    &sig,
                    false,
                )
                .await;
                // Send it to be proved (block current thread).
                // Persist proof.
                // Check now - last proof time > MAX_SINCE_LAST_PROOF.
                // If yes, prove the nova proof for groth16 and issue transaction.
            }
        }

        delivery
            .ack(BasicAckOptions::default())
            .await
            .expect("basic_ack");
    }
}
