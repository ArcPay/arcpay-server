use async_graphql::async_trait::async_trait;
use num_bigint::BigInt;
use pg_bigdecimal::{PgNumeric, BigDecimal};
use pmtree::PmtreeErrorKind::DatabaseError;
use std::collections::HashMap;
use pmtree::{DBKey, Value, PmtreeResult, DatabaseErrorKind, Database, Hasher};
use rln::{hashers::PoseidonHash, utils::{fr_to_bytes_be, bytes_be_to_fr}};
use rln::circuit::Fr as Fp;
use tokio_postgres::Client;


/// If we want different parameters to be stored at the time of new,load and get,put:
/// create a new struct `PostgresDB` and impl Database for PostgresDB.
pub(crate) struct PostgresDBConfig {
    pub client: Client,
    pub tablename: String,
}

#[async_trait]
impl Database for PostgresDBConfig {
    type Config = PostgresDBConfig;

    async fn new(db_config: PostgresDBConfig) -> PmtreeResult<Self> {
        let query = format!("CREATE TABLE {} (
            leaf NUMERIC(78,0) NOT NULL,
            index NUMERIC(20,0) NOT NULL,
            PRIMARY KEY (index)
        );", db_config.tablename);

        let statement = db_config.client.prepare(&query).await.unwrap();

        db_config.client.execute(&statement, &[]).await.unwrap();

        Ok(PostgresDBConfig { client: db_config.client, tablename: db_config.tablename })
    }

    async fn load(db_config: PostgresDBConfig) -> PmtreeResult<Self> {
        Ok(db_config)
    }

    // TODO: decide if get and put fn return error instead of panic on db error.
    async fn get(&mut self, key: DBKey) -> PmtreeResult<Option<Value>> {
        let index = PgNumeric::new(Some(BigDecimal::new(BigInt::from_bytes_be(num_bigint::Sign::Plus, &key),0)));
        dbg!(&index);

        let query = format!("SELECT leaf FROM {} WHERE index=$1", self.tablename);

        let rows = self.client.query(&query, &[&index]).await.unwrap();
        dbg!(&rows);
        dbg!(rows.len());
        assert!(rows.len() <= 1, "key should be unique");

        let ret = match rows.len() {
            1 => {dbg!("here"); let x = to_bytes_vec(rows[0].get("leaf")); dbg!(&x); Ok(Some(x))},
            0 => {dbg!("h1"); Ok(None)},
            _ => {dbg!("h1e"); Err(DatabaseError(DatabaseErrorKind::CustomError("Primary key should be unique".to_string())))}
        };
        dbg!(&ret);
        ret
    }

    async fn put(&mut self, key: DBKey, value: Value) -> PmtreeResult<()> {
        let index = PgNumeric::new(Some(BigDecimal::new(BigInt::from_bytes_be(num_bigint::Sign::Plus, &key),0)));
        let leaf = PgNumeric::new(Some(BigDecimal::new(BigInt::from_bytes_be(num_bigint::Sign::Plus, &value),0)));

        // If `index` already exists in the table, update `leaf` column.
        let query = format!(
            "INSERT INTO {} (leaf, index)
            VALUES ($1, $2)
            ON CONFLICT (index)
            DO UPDATE SET leaf=EXCLUDED.leaf",
            self.tablename
        );

        let rows_modified = self.client.execute(&query, &[&leaf, &index]).await.unwrap();
        assert_eq!(rows_modified, 1, "should be only 1 new row");
        Ok(())
    }

    async fn put_batch(&mut self, _subtree: HashMap<DBKey, Value>) -> PmtreeResult<()> {
        Err(DatabaseError(DatabaseErrorKind::CustomError("TODO: support put_batch".to_string())))
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
        dbg!(&value);
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
    println!("{}", n.to_string());
    assert!(n.is_integer());
    let (num, scale) = n.as_bigint_and_exponent();
    assert_eq!(scale, 0);
    let buint = num.to_biguint().unwrap();
    let vec = buint.to_bytes_be();
    vec
}
