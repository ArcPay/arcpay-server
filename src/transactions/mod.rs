use self::primitive::PrimitiveTransaction;

mod multicoin;
mod primitive;

pub trait RichTransaction {
    type Error: std::fmt::Debug; // TODO: do we also need to implement Error, Send, and Sync?

    fn decompose(&self) -> Vec<PrimitiveTransaction>;
    fn authorized(&self) -> Result<(), Self::Error>;
}
