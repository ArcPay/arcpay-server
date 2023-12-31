use ethers::prelude::U256;
use futures_lite::stream::StreamExt;
use lapin::{options::BasicAckOptions, Consumer};
use pmtree::Hasher;
use rln::circuit::Fr;
use secp256k1::{ecdsa, Message, PublicKey, Secp256k1};
use sha3::{Digest, Keccak256};
use tokio::sync::RwLock;
use tokio_postgres::IsolationLevel;

use crate::{
    contract_owner::ContractOwner,
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
    let mut mint_time: U256 = U256::default();
    let arcpay_owner = ContractOwner::new().await.unwrap();
    while let Some(delivery) = consumer.next().await {
        let delivery = delivery.expect("error in consumer");
        let mesg: QueueMessage = bincode::deserialize(delivery.data.as_slice())
            .expect("deserialization should be correct");
        delivery
            .ack(BasicAckOptions::default())
            .await
            .expect("basic_ack");

        // Send `mesg` it to be proved (block current thread).
        // Persist proof.

        let mut mt = mt.write().await;
        match mesg {
            QueueMessage::Mint((leaf, timestamp)) => {
                mint_time = timestamp;
                // TODO check what happens when amount overflows u64.
                mint_in_merkle(&mut mt, leaf).await;
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
            }
            QueueMessage::Withdraw((leaf, index, sig)) => {
                mt.set(index, MyPoseidon::default_leaf(), None)
                    .await
                    .unwrap();
            }
        }

        let state_root = MyPoseidon::serialize(mt.root());
        // drop(mt);
        // Check now - last proof time > MAX_SINCE_LAST_PROOF.
        // If yes, prove the nova proof for groth16 and then issue the below transaction:
        {
            let state_root = U256::from_big_endian(&state_root);

            let state_root_updated = arcpay_owner.update_state_root(state_root, mint_time).await;
            let finalized_state_root = arcpay_owner.get_state_root().await;
            if let Err(_update_err) = state_root_updated {
                dbg!(&_update_err);
                match finalized_state_root {
                    Err(_get_state_err) => {
                        todo!("issue alert; keep building on the same proof and retry on the same iteration");
                        // keep building on the same proof and retry in the next iteration.
                    }
                    Ok(root) => {
                        if root != state_root {
                            todo!("issue alert; keep building on the same proof and retry on the same iteration");
                        } else {
                            todo!("root has been updated, behave like there was no error while updating the root");
                        }
                    }
                }
            }

            // update the finalized merkle tree by copying the proven merkle dbs to finalized dbs.
            // may be optimized later once we have large tables.
            {
                let mut client = mt.db.client.write().await;
                let tx = client
                    .build_transaction()
                    .isolation_level(IsolationLevel::Serializable)
                    .start()
                    .await
                    .expect("send_consumer:final build_transaction.start() error");

                let query = format!(
                    "TRUNCATE {fin_merkle_table};",
                    fin_merkle_table = "fin_merkle"
                );

                let statement = tx.prepare(&query).await.unwrap();
                tx.execute(&statement, &[]).await.unwrap();
                let query = format!(
                    "TRUNCATE {fin_pre_image_table};",
                    fin_pre_image_table = "fin_pre_image"
                );

                let statement = tx.prepare(&query).await.unwrap();
                tx.execute(&statement, &[]).await.unwrap();

                let query = format!(
                    "INSERT INTO {fin_merkle_table} SELECT * FROM {merkle_table};",
                    fin_merkle_table = "fin_merkle",
                    merkle_table = "merkle"
                );

                let statement = tx.prepare(&query).await.unwrap();
                tx.execute(&statement, &[]).await.unwrap();
                let query = format!(
                    "INSERT INTO {fin_pre_image_table} SELECT * FROM {pre_image_table};",
                    fin_pre_image_table = "fin_pre_image",
                    pre_image_table = "pre_image"
                );

                let statement = tx.prepare(&query).await.unwrap();
                tx.execute(&statement, &[]).await.unwrap();

                tx.commit().await.expect("fin::copy error");
            }
        }
    }
}
