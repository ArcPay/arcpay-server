use async_graphql::async_trait::async_trait;
use num_bigint::BigInt;
use pg_bigdecimal::{BigDecimal, PgNumeric};
use pmtree::tree::Key;
use pmtree::PmtreeErrorKind::DatabaseError;
use pmtree::{DBKey, Database, DatabaseErrorKind, Hasher, PmtreeResult, Value};
use rln::circuit::Fr as Fp;
use rln::{
    hashers::PoseidonHash,
    utils::{bytes_be_to_fr, fr_to_bytes_be},
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use tokio_postgres::{Client, IsolationLevel};

use crate::model::{CoinRange, Leaf};

/// If we want different parameters to be stored at the time of new,load and get,put:
/// create a new struct `PostgresDB` and impl Database for PostgresDB.
#[derive(Clone)]
pub(crate) struct PostgresDBConfig {
    pub client: Arc<RwLock<Client>>,
    pub merkle_table: String,
    pub pre_image_table: String,
}

impl PostgresDBConfig {
    pub(crate) async fn get_for_address(&self, address: &[u8; 20]) -> Vec<CoinRange> {
        let client = self.client.read().await;
        let address = PgNumeric::new(Some(BigDecimal::new(
            BigInt::from_bytes_be(num_bigint::Sign::Plus, address),
            0,
        )));
        let query = format!(
            "SELECT leaf, coin_low, coin_high from {} WHERE owner = $1",
            self.pre_image_table
        );

        let rows = client.query(&query, &[&address]).await.unwrap();
        let mut collection = vec![];
        for row in rows {
            let leaf: PgNumeric = row.get("leaf");
            let low_coin = to_bytes_vec(row.get("coin_low"));
            let high_coin = to_bytes_vec(row.get("coin_high"));

            let query = format!("SELECT key from {} WHERE leaf=$1", self.merkle_table);

            let key = client.query(&query, &[&leaf]).await.unwrap();
            if !key.is_empty() {
                assert_eq!(key.len(), 1);
                let key: DBKey = to_byte_array(to_bytes_vec(key[0].get("key")));
                let key: Key = key.into();
                collection.push(CoinRange {
                    index: key.1,
                    low_coin: u64::from_be_bytes(to_byte_array(low_coin)),
                    high_coin: u64::from_be_bytes(to_byte_array(high_coin)),
                });
            }
        }

        collection
    }
}

#[async_trait]
impl Database for PostgresDBConfig {
    type Config = PostgresDBConfig;
    type PreImage = Leaf;

    async fn new(db_config: PostgresDBConfig) -> PmtreeResult<Self> {
        {
            let mut client = db_config.client.write().await;
            let tx = client
                .build_transaction()
                .isolation_level(IsolationLevel::Serializable)
                .start()
                .await
                .expect("Database::new build_transaction.start() error");

            //  TODO remove drop
            let query = format!(
                "DROP TABLE IF EXISTS {merkle_table};",
                merkle_table = db_config.merkle_table
            );

            let statement = tx.prepare(&query).await.unwrap();
            tx.execute(&statement, &[]).await.unwrap();
            let query = format!(
                "CREATE TABLE {merkle_table} (
                    leaf NUMERIC(78,0) NOT NULL,
                    key NUMERIC(20,0) NOT NULL,
                    PRIMARY KEY (key)
                );",
                merkle_table = db_config.merkle_table
            );

            let statement = tx.prepare(&query).await.unwrap();
            tx.execute(&statement, &[]).await.unwrap();

            //  TODO remove drop
            let query = format!(
                "DROP TABLE if exists {pre_image_table};",
                pre_image_table = db_config.pre_image_table
            );

            let statement = tx.prepare(&query).await.unwrap();
            tx.execute(&statement, &[]).await.unwrap();
            let query = format!(
                "CREATE TABLE {pre_image_table} (
                    leaf NUMERIC(78,0) NOT NULL, -- 32 bytes
                    owner NUMERIC(49,0) NOT NULL, -- 20 bytes
                    coin_low NUMERIC(13,0) NOT NULL, -- 32 bytes
                    coin_high NUMERIC(13,0) NOT NULL, -- 32 bytes
                    PRIMARY KEY (leaf)
                );",
                pre_image_table = db_config.pre_image_table
            );

            let statement = tx.prepare(&query).await.unwrap();
            tx.execute(&statement, &[]).await.unwrap();

            tx.commit().await.expect("Database::new create table error");
        }

        Ok(db_config)
    }

    async fn load(db_config: PostgresDBConfig) -> PmtreeResult<Self> {
        Ok(db_config)
    }

    // TODO: decide if get and put fn return error instead of panic on db error.
    async fn get(&self, key: DBKey) -> PmtreeResult<Option<Value>> {
        let key = PgNumeric::new(Some(BigDecimal::new(
            BigInt::from_bytes_be(num_bigint::Sign::Plus, &key),
            0,
        )));

        let query = format!("SELECT leaf FROM {} WHERE key=$1", self.merkle_table);

        let client = self.client.read().await;

        let rows = client.query(&query, &[&key]).await.unwrap();
        assert!(rows.len() <= 1, "key should be unique");

        match rows.len() {
            1 => Ok(Some(to_bytes_vec(rows[0].get("leaf")))),
            0 => Ok(None),
            _ => Err(DatabaseError(DatabaseErrorKind::CustomError(
                "Primary key should be unique".to_string(),
            ))),
        }
    }

    async fn get_pre_image(&self, key: DBKey) -> PmtreeResult<Option<Self::PreImage>> {
        let key = PgNumeric::new(Some(BigDecimal::new(
            BigInt::from_bytes_be(num_bigint::Sign::Plus, &key),
            0,
        )));

        let query = format!(
            "SELECT {pre_image}.owner, {pre_image}.coin_low, {pre_image}.coin_high
            FROM {pre_image} JOIN {merkle}
            ON {pre_image}.leaf = {merkle}.leaf
            WHERE {merkle}.key=$1",
            pre_image = self.pre_image_table,
            merkle = self.merkle_table
        );

        let client = self.client.read().await;

        let rows = client.query(&query, &[&key]).await.unwrap();
        assert!(rows.len() <= 1, "key should be unique");

        match rows.len() {
            1 => {
                let owner = to_bytes_vec(rows[0].get("owner"));
                let low_coin = to_bytes_vec(rows[0].get("coin_low"));
                let high_coin = to_bytes_vec(rows[0].get("coin_high"));

                Ok(Some(Leaf {
                    address: to_byte_array(owner),
                    low_coin: u64::from_be_bytes(to_byte_array(low_coin)),
                    high_coin: u64::from_be_bytes(to_byte_array(high_coin)),
                }))
            }
            0 => Ok(None),
            _ => Err(DatabaseError(DatabaseErrorKind::CustomError(
                "Primary key should be unique".to_string(),
            ))),
        }
    }

    async fn put_with_pre_image(
        &mut self,
        key: DBKey,
        value: Value,
        pre_image: Option<Self::PreImage>,
    ) -> PmtreeResult<()> {
        match pre_image {
            Some(t) => {
                // TODO: add a check to verify hash(pre_image) == value
                let key = PgNumeric::new(Some(BigDecimal::new(
                    BigInt::from_bytes_be(num_bigint::Sign::Plus, &key),
                    0,
                )));
                let leaf = PgNumeric::new(Some(BigDecimal::new(
                    BigInt::from_bytes_be(num_bigint::Sign::Plus, &value),
                    0,
                )));
                let owner = PgNumeric::new(Some(BigDecimal::new(
                    BigInt::from_bytes_be(num_bigint::Sign::Plus, &t.address),
                    0,
                )));
                let coin_low = PgNumeric::new(Some(BigDecimal::new(
                    BigInt::from_bytes_be(num_bigint::Sign::Plus, &t.low_coin.to_be_bytes()),
                    0,
                )));
                let coin_high = PgNumeric::new(Some(BigDecimal::new(
                    BigInt::from_bytes_be(num_bigint::Sign::Plus, &t.high_coin.to_be_bytes()),
                    0,
                )));

                let mut client = self.client.write().await;

                let tx = client
                    .build_transaction()
                    .isolation_level(IsolationLevel::Serializable)
                    .start()
                    .await
                    .expect("Database::put_with_pre_image build_transaction error");

                // If `key` already exists in the table, update `leaf` column.
                let query = format!(
                    "INSERT INTO {} (leaf, key)
                    VALUES ($1, $2)
                    ON CONFLICT (key)
                    DO UPDATE SET leaf=EXCLUDED.leaf",
                    self.merkle_table
                );
                let statement = tx.prepare(&query).await.unwrap();

                let rows_modified = tx.execute(&statement, &[&leaf, &key]).await.unwrap();
                assert_eq!(rows_modified, 1, "should be only 1 new row");

                let query = format!(
                    "INSERT INTO {} (leaf, owner, coin_low, coin_high)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (leaf)
                    DO NOTHING",
                    self.pre_image_table
                );
                let statement = tx.prepare(&query).await.unwrap();

                let rows_modified = tx
                    .execute(&statement, &[&leaf, &owner, &coin_low, &coin_high])
                    .await
                    .unwrap();
                assert_eq!(rows_modified, 1, "should be only 1 new row");
                tx.commit().await.unwrap();
                Ok(())
            }
            None => Self::put(self, key, value).await,
        }
    }

    async fn put(&mut self, key: DBKey, value: Value) -> PmtreeResult<()> {
        let key = PgNumeric::new(Some(BigDecimal::new(
            BigInt::from_bytes_be(num_bigint::Sign::Plus, &key),
            0,
        )));
        let leaf = PgNumeric::new(Some(BigDecimal::new(
            BigInt::from_bytes_be(num_bigint::Sign::Plus, &value),
            0,
        )));

        // If `index` already exists in the table, update `leaf` column.
        let query = format!(
            "INSERT INTO {} (leaf, key)
            VALUES ($1, $2)
            ON CONFLICT (key)
            DO UPDATE SET leaf=EXCLUDED.leaf",
            self.merkle_table
        );

        let client = self.client.write().await;

        let rows_modified = client.execute(&query, &[&leaf, &key]).await.unwrap();
        assert_eq!(rows_modified, 1, "should be only 1 new row");
        Ok(())
    }

    async fn put_batch(&mut self, _subtree: HashMap<DBKey, Value>) -> PmtreeResult<()> {
        Err(DatabaseError(DatabaseErrorKind::CustomError(
            "TODO: support put_batch".to_string(),
        )))
    }
}

pub(crate) struct MyPoseidon(PoseidonHash);

impl Hasher for MyPoseidon {
    type Fr = Fp;

    fn default_leaf() -> Self::Fr {
        Self::Fr::from(0)
    }

    fn serialize(value: Self::Fr) -> Value {
        fr_to_bytes_be(&value)
    }

    fn deserialize(value: Value) -> Self::Fr {
        let mut value = value;

        // If hash is less than 32 bytes (if there are zeros at the beginning),
        // `bytes_be_to_fr()` panics because it expects a fixed size vector.
        // Hence, we increase the size to 32 here.
        if value.len() < 32 {
            let pre_len = value.len();
            value.resize(32, 0);
            value.rotate_right(32 - pre_len);
        }
        bytes_be_to_fr(&value).0 // TODO: confirm if .1 is required
    }

    fn hash(input: &[Self::Fr]) -> Self::Fr {
        <PoseidonHash as utils::merkle_tree::Hasher>::hash(input)
    }
}

fn to_bytes_vec(from: PgNumeric) -> Vec<u8> {
    let n = from.n.unwrap();
    assert!(n.is_integer());
    let (num, scale) = n.as_bigint_and_exponent();
    assert_eq!(scale, 0);
    let buint = num.to_biguint().unwrap();
    buint.to_bytes_be()
}

fn to_byte_array<const N: usize>(from: Vec<u8>) -> [u8; N] {
    let mut array = [0u8; N];
    let source_len = from.len();

    if source_len >= N {
        let start_index = source_len - N;
        let end_index = source_len;
        array.copy_from_slice(&from[start_index..end_index]);
    } else {
        let start_index = N - source_len;
        let end_index = N;
        array[start_index..end_index].copy_from_slice(&from);
    }

    array
}
