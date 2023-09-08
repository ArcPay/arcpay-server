use crate::merkle::MyPoseidon;
use async_graphql::InputObject;
use pmtree::Hasher;
use rln::circuit::Fr;
use serde::{Deserialize, Serialize};

// Primitive transactions is the basic form of transaction that is processed by the main
// state transition circuit. All other transaction types ultimately compile down into
// primitive transactions.
//
// The primitive transaction spends a single leaf, giving the first part to the receiver,
// the second part to the operator, and the third part back to the sender.
// Note, the lowest_coin is not strictly necessary because it's uniquely specified by the leaf_id,
// but it's convenient for validation purposes.
#[derive(Debug, InputObject, Serialize, Deserialize, Clone)]
pub struct PrimitiveTransaction {
    sender: [u8; 20],
    receiver: [u8; 20],
    leaf_id: u64,
    low_coin: u64,
    high_coin: u64,
    upper_bound: u64,
    fee_upper_bound: u64,
}

#[derive(Debug)]
pub enum PrimitiveTransactionError {
    NegativeAmountSent {
        low_coin: u64,
        upper_bound: u64,
    },
    NegativeFee {
        upper_bound: u64,
        fee_upper_bound: u64,
    },
    UnavailabeFunds {
        high_coin: u64,
        fee_upper_bound: u64,
    },
}

impl PrimitiveTransaction {
    pub fn valid(&self) -> Result<(), PrimitiveTransactionError> {
        if self.low_coin > self.upper_bound {
            Err(PrimitiveTransactionError::NegativeAmountSent {
                low_coin: self.low_coin,
                upper_bound: self.upper_bound,
            })
        } else if self.upper_bound > self.fee_upper_bound {
            Err(PrimitiveTransactionError::NegativeFee {
                upper_bound: self.upper_bound,
                fee_upper_bound: self.fee_upper_bound,
            })
        } else if self.high_coin < self.fee_upper_bound {
            Err(PrimitiveTransactionError::UnavailabeFunds {
                high_coin: self.high_coin,
                fee_upper_bound: self.fee_upper_bound,
            })
        } else {
            Ok(())
        }
    }

    pub fn hash(&self) -> Fr {
        MyPoseidon::hash(&[
            MyPoseidon::deserialize(self.sender.to_vec()),
            MyPoseidon::deserialize(self.receiver.to_vec()),
            Fr::from(self.leaf_id),
            Fr::from(self.low_coin),
            Fr::from(self.high_coin),
            Fr::from(self.upper_bound),
            Fr::from(self.fee_upper_bound),
        ])
    }

    // Getters (TODO: use getset crate)
    pub fn sender(&self) -> [u8; 20] {
        self.sender
    }

    pub fn receiver(&self) -> [u8; 20] {
        self.receiver
    }

    pub fn leaf_id(&self) -> u64 {
        self.leaf_id
    }

    pub fn low_coin(&self) -> u64 {
        self.low_coin
    }

    pub fn high_coin(&self) -> u64 {
        self.high_coin
    }

    pub fn upper_bound(&self) -> u64 {
        self.upper_bound
    }

    pub fn fee_upper_bound(&self) -> u64 {
        self.fee_upper_bound
    }
}
