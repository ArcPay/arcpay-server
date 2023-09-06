use crate::{merkle::MyPoseidon, model::Signature};
use async_graphql::InputObject;
use ethers::prelude::{Eip712, EthAbiType};
use ethers::types::transaction::eip712::Eip712;
use ethers::types::SignatureError;
use pmtree::Hasher;
use rln::circuit::Fr;
use serde::{Deserialize, Serialize};

use super::primitive::{PrimitiveTransaction, PrimitiveTransactionError};
use super::RichTransaction;

// Multi coin sends get around the "change" problem, where a user
// has loose change in the form of many coin ranges. A multi coin send
// lets the user spend multiple coin ranges with a single signature.
//
// MultiCoinSend is the succinct representation of the transactions that
// is signed by the spender.
#[derive(Debug, InputObject, Serialize, Deserialize, Eip712, EthAbiType, Clone)]
#[eip712(
    name = "ArcPay",
    version = "0",
    chain_id = 11155111,
    verifying_contract = "0x21843937646d779e1e27a5f94ff5972f80c942bd"
)]
pub struct MultiCoinSend {
    receiver: [u8; 20],
    amount: u64,
    fee: u64,
    detail: [u8; 32],
}

// A SignedMultiCoinSend is a full representation of the multi coin send with
// enough information to tell if the child transactions were properly authorized
//
// TODO: split out summary/detail vs signature validation logic so that we can easily implement multicoin transactions for any account type
#[derive(Debug, InputObject, Serialize, Deserialize)]
pub struct SignedMultiCoinSend {
    summary: MultiCoinSend,
    signature: Signature,
    child_transactions: Vec<PrimitiveTransaction>,
}

#[derive(Debug)]
pub enum MultiSendAuthorizationError {
    WrongHash { claimed: Fr, actual: Fr },
    EIP712Error(<MultiCoinSend as ethers::types::transaction::eip712::Eip712>::Error),
    InvalidSignature(SignatureError),
    InvalidChildTransaction(PrimitiveTransactionError),
    WrongSender { signer: [u8; 20], sender: [u8; 20] },
    DifferentReceivers([u8; 20], [u8; 20]),
    SpendMismatch { claimed: u64, actual: u64 },
    FeeMismatch { claimed: u64, actual: u64 },
}

impl RichTransaction for SignedMultiCoinSend {
    type Error = MultiSendAuthorizationError;

    fn decompose(&self) -> Vec<PrimitiveTransaction> {
        self.child_transactions.to_vec()
    }

    // Checks whether the child transactions are appropriately authorized by the top level signature.
    // The summary must correctly represent the child transactions, or the user hasn't given informed consent.
    // This will be checked in the authorization circuit, and doesn't guarantee that the transactions are valid
    // for a given state, as the signer may not own the leaves they're trying to spend.
    fn authorized(&self) -> Result<(), MultiSendAuthorizationError> {
        // Ensure that the given child transactions hash to the claimed hash
        if self.child_hash() != MyPoseidon::deserialize(self.summary.detail.to_vec()) {
            return Err(MultiSendAuthorizationError::WrongHash {
                claimed: self.child_hash(),
                actual: MyPoseidon::deserialize(self.summary.detail.to_vec()),
            });
        }

        // Check the signature
        let msg_hash = match self.summary.encode_eip712() {
            Ok(m) => m,
            Err(e) => return Err(MultiSendAuthorizationError::EIP712Error(e)),
        };

        let ethsig = ethers::prelude::Signature::from(self.signature.clone());
        let signer = match ethsig.recover(msg_hash) {
            Ok(s) => s,
            Err(e) => return Err(MultiSendAuthorizationError::InvalidSignature(e)),
        };

        for child in self.child_transactions.iter() {
            // Ensure that every child transaction is valid
            match child.valid() {
                Ok(()) => (),
                Err(e) => return Err(MultiSendAuthorizationError::InvalidChildTransaction(e)),
            }

            // Ensure that the sender of every child transaction is the signer
            let signer_bytes: [u8; 20] = signer.into();
            if child.sender() != signer_bytes {
                return Err(MultiSendAuthorizationError::WrongSender {
                    signer: signer_bytes,
                    sender: child.sender(),
                });
            }

            // Ensure that the receiver of every child transaction is the receiver in the top level transaction
            if child.receiver() != self.summary.receiver {
                return Err(MultiSendAuthorizationError::DifferentReceivers(
                    child.receiver(),
                    self.summary.receiver,
                ));
            }
        }

        // Ensure that the claimed amount spend is the sum of the child transactions
        let sum = self.child_transactions.iter().fold(
            0u64,
            |acc: u64, next: &PrimitiveTransaction| -> u64 {
                acc + next.upper_bound() - next.low_coin()
            },
        );
        if sum != self.summary.amount {
            return Err(MultiSendAuthorizationError::SpendMismatch {
                claimed: self.summary.amount,
                actual: sum,
            });
        }

        // The claimed amount spend is different to the of the child transactions
        let sum = self.child_transactions.iter().fold(
            0u64,
            |acc: u64, next: &PrimitiveTransaction| -> u64 {
                acc + next.fee_upper_bound() - next.upper_bound()
            },
        );
        if sum != self.summary.fee {
            return Err(MultiSendAuthorizationError::SpendMismatch {
                claimed: self.summary.fee,
                actual: sum,
            });
        }

        Ok(())
    }
}

impl SignedMultiCoinSend {
    fn child_hash(&self) -> Fr {
        self.child_transactions
            .iter()
            .fold(Fr::from(0), |acc: Fr, next: &PrimitiveTransaction| -> Fr {
                MyPoseidon::hash(&[next.hash(), acc])
            })
    }
}
