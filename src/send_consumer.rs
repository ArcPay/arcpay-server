use ethers::abi::{encode, Token};
use ethers::types::transaction::eip712::{EIP712Domain, Eip712};

use ethers::utils::keccak256;
use ethers::{
    prelude::{Eip712, EthAbiType, U256},
    types::Address,
};
use futures_lite::stream::StreamExt;
use lapin::{options::BasicAckOptions, Consumer};
use pmtree::Hasher;

use tokio::sync::RwLock;
use tokio_postgres::IsolationLevel;

use crate::QueueMessage;
use crate::{
    contract_owner::ContractOwner,
    merkle::{MyPoseidon, PostgresDBConfig},
    model::{mint_in_merkle, Leaf},
    model::{send_in_merkle, Signature},
};

#[derive(Eip712, EthAbiType, Clone, Debug)]
#[eip712(
    name = "ArcPay",
    version = "0",
    chain_id = 11155111,
    verifying_contract = "0x21843937646d779E1e27A5f94fF5972F80C942bD"
)]
struct Send712 {
    owner: Address,
    low_coin: U256,
    high_coin: U256,
    highest_coin_to_send: U256,
    receiver: Address,
}

////// remove this debug trait //////
// trait TraitName {
//     // Compute the domain separator;
//     // See: https://github.com/gakonst/ethers-rs/blob/master/examples/permit_hash.rs#L41
//     fn separato(&self) -> [u8; 32];
// }

// impl TraitName for EIP712Domain {
//     // Compute the domain separator;
//     // See: https://github.com/gakonst/ethers-rs/blob/master/examples/permit_hash.rs#L41
//     fn separato(&self) -> [u8; 32] {
//         // full name is `EIP712Domain(string name,string version,uint256 chainId,address
//         // verifyingContract,bytes32 salt)`
//         let mut ty = "EIP712Domain(".to_string();

//         let mut tokens = Vec::new();
//         let mut needs_comma = false;
//         if let Some(ref name) = self.name {
//             ty += "string name";
//             tokens.push(Token::Uint(U256::from(keccak256(name))));
//             needs_comma = true;
//         }

//         if let Some(ref version) = self.version {
//             if needs_comma {
//                 ty.push(',');
//             }
//             ty += "string version";
//             tokens.push(Token::Uint(U256::from(keccak256(version))));
//             needs_comma = true;
//         }

//         if let Some(chain_id) = self.chain_id {
//             if needs_comma {
//                 ty.push(',');
//             }
//             ty += "uint256 chainId";
//             tokens.push(Token::Uint(chain_id));
//             needs_comma = true;
//         }

//         if let Some(verifying_contract) = self.verifying_contract {
//             if needs_comma {
//                 ty.push(',');
//             }
//             ty += "address verifyingContract";
//             tokens.push(Token::Address(verifying_contract));
//             needs_comma = true;
//         }

//         if let Some(salt) = self.salt {
//             if needs_comma {
//                 ty.push(',');
//             }
//             ty += "bytes32 salt";
//             tokens.push(Token::Uint(U256::from(salt)));
//         }

//         ty.push(')');

//         tokens.insert(0, Token::Uint(U256::from(keccak256(ty))));
//         dbg!(&tokens);
//         dbg!(&encode(&tokens));
//         keccak256(encode(&tokens))
//     }
// }
///////////////////////////

/// Verify signature and public key in `sig` is correct.
pub(crate) fn verify_ecdsa(
    leaf: &Leaf,
    highest_coin_to_send: u64,
    receiver: &[u8; 20],
    sig: Signature,
) {
    let receiver: Address = receiver.into();
    let msg = Send712 {
        owner: leaf.address.into(),
        low_coin: leaf.low_coin.into(),
        high_coin: leaf.high_coin.into(),
        highest_coin_to_send: highest_coin_to_send.into(),
        receiver,
    };

    dbg!(&msg);

    // dbg!(&msg.domain().unwrap().separato());
    dbg!(&msg.struct_hash().unwrap());
    let msg_hash = msg.encode_eip712().unwrap();
    dbg!("712", msg_hash);

    let ethsig = ethers::prelude::Signature::from(sig);
    dbg!(&ethsig);
    let signer = ethsig.recover(msg_hash).unwrap();
    assert_eq!(signer, leaf.address.into());
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
                let (leaf, index, highest_coin_to_send, recipient, proofs) = send;
                send_in_merkle(
                    &mut mt,
                    index as u64,
                    &leaf,
                    highest_coin_to_send,
                    &recipient,
                    false,
                )
                .await;
            }
            QueueMessage::Withdraw((leaf, index)) => {
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
            /*
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
            } */
        }
    }
}
