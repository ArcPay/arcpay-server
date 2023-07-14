use num_bigint::BigInt;
use pg_bigdecimal::{PgNumeric, BigDecimal};
use tokio_postgres::{Client, Error};
use tokio::sync::RwLock;
use std::sync::Arc;


pub(crate) struct UserBalanceConfig {
    pub client: Arc<RwLock<Client>>,
    pub tablename: String,
}

impl UserBalanceConfig {
    pub async fn is_balance_sufficient(&self, owner: &[u8; 20], amount: &[u8; 8]) -> Result<bool, Error> {
        let index = PgNumeric::new(Some(BigDecimal::new(BigInt::from_bytes_be(num_bigint::Sign::Plus, owner),0)));
        dbg!(&index);

        let query = format!("SELECT balance FROM {} WHERE owner=$1", self.tablename);

        let client = self.client.read().await;

        let rows = client.query(&query, &[&index]).await?;
        dbg!(&rows);
        dbg!(rows.len());
        assert!(rows.len() <= 1, "key should be unique");

        match rows.len() {
            1 => {
                let x: PgNumeric = rows[0].get("balance");
                let amount = PgNumeric::new(Some(BigDecimal::new(BigInt::from_bytes_be(num_bigint::Sign::Plus, amount),0)));
                Ok(x.ge(&amount))
            }
            0 => Ok(false),
            _ => panic!("user_balance primary key should be unique")
        }
    }
}
