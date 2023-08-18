use async_graphql::{EmptySubscription, Schema};
use ethers::prelude::*;

use axum::{extract::Extension, routing::get, Router, Server};
use lapin::Channel;
use serde::{Deserialize, Serialize};
use tokio_postgres::NoTls;
use user_balance::UserBalanceConfig;

use model::{Leaf, MutationRoot, SendMessageType, WithdrawMessageType};
use std::sync::Arc;
use tokio::sync::RwLock;

use lapin::{options::*, types::FieldTable, Connection, ConnectionProperties};

use clap::Parser;
use eyre::Result; // TODO replace .unwrap() with `?` and `wrap_err` from eyre::Result.

use crate::{
    merkle::{MyPoseidon, PostgresDBConfig},
    mint::mint,
    model::QueryRoot,
    routes::{graphql_handler, graphql_playground, health},
};
use pmtree::MerkleTree;
mod contract_owner;
mod merkle;
mod mint;
mod model;
mod routes;
mod send_consumer;
mod user_balance;

#[derive(Parser, Debug)]
struct Cli {
    /// Create or load a merkle tree
    #[arg(long)]
    merkle: MerkleCommand,
    // Activates the mint pipeline
    // #[arg(long, action=clap::ArgAction::SetTrue)]
    // mint: bool,
}

#[derive(clap::ValueEnum, Debug, Clone)]
enum MerkleCommand {
    New,
    Load,
}

struct ApiContext {
    user_balance_db: UserBalanceConfig,
    mt: Arc<RwLock<MerkleTree<PostgresDBConfig, MyPoseidon>>>,
    channel: Arc<Channel>,
}

const MERKLE_DEPTH: usize = 32; // TODO: read in from parameters file
const ARCPAY_ADDRESS: &str = "0x82B766D0a234489a299BBdA3DBe6ba206d77D35F";

const QUEUE_NAME: &str = "user_requests";

/// Maximum time gap (in seconds) between two proof submissions.
/// Note that it's not strict and depends on the number of requests.
/// Set it half the max time set in the contract.
const MAX_SINCE_LAST_PROVE: usize = 30; // TODO adjust based on traffic

abigen!(ArcPayContract, "ArcPay.json");

#[derive(Debug, Serialize, Deserialize)]
enum QueueMessage {
    Mint((Leaf, U256)),
    Send(SendMessageType),
    Withdraw(WithdrawMessageType),
}

#[tokio::main]
async fn main() -> Result<()> {
    //////////////// experimental RabbitMQ integration ////////////////
    // Connect to the RabbitMQ server
    let addr = "amqp://guest:guest@localhost:5672/%2f";
    let conn = Connection::connect(addr, ConnectionProperties::default()).await?;

    // Create a channel
    let channel = conn.create_channel().await?;

    // TODO: declare queue as persistent through `QueueDeclareOptions`. See message durability section:
    // https://www.rabbitmq.com/tutorials/tutorial-two-python.html.
    // Also make messages persistent by updating publisher. The value `PERSISTENT_DELIVERY_MODE` is 2,
    // see: https://pika.readthedocs.io/en/stable/_modules/pika/spec.html.
    // Even this doesn't fully guarantee message persistence, see "Note on message persistence"
    // in the tutorial which links to: https://www.rabbitmq.com/confirms.html#publisher-confirms.
    // We have enabled publisher confirms in the next step.
    channel
        .queue_declare(
            QUEUE_NAME,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;
    // Enable publisher confirms
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await?;

    // Consume messages from the queue
    let consumer = channel
        .basic_consume(
            QUEUE_NAME,
            "my_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("basic_consume");

    println!("Waiting for messages...");

    let cli = Cli::parse();
    let mut handles = vec![];
    let channel = Arc::new(channel);

    // if let Some(t) = cli.merkle {
    //////////////// experimental GraphQL integration /////////////////
    let (client, connection) =
        tokio_postgres::connect("host=localhost user=dev dbname=arcpay", NoTls).await?;

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
        merkle_table: "merkle".to_string(),
        pre_image_table: "pre_image".to_string(),
    };

    let (proven_client, proven_connection) =
        tokio_postgres::connect("host=localhost user=dev dbname=proven_arcpay", NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = proven_connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let proven_client = Arc::new(RwLock::new(proven_client));

    let proven_db_config = PostgresDBConfig {
        client: proven_client,
        merkle_table: "merkle".to_string(),
        pre_image_table: "pre_image".to_string(),
    };

    let mt = match cli.merkle {
        MerkleCommand::New => (
            MerkleTree::<PostgresDBConfig, MyPoseidon>::new(MERKLE_DEPTH, db_config).await?,
            MerkleTree::<PostgresDBConfig, MyPoseidon>::new(MERKLE_DEPTH, proven_db_config).await?,
        ),
        MerkleCommand::Load => (
            MerkleTree::<PostgresDBConfig, MyPoseidon>::load(db_config).await?,
            MerkleTree::<PostgresDBConfig, MyPoseidon>::load(proven_db_config).await?,
        ),
    };

    handles.push(tokio::spawn(async move {
        send_consumer::send_consumer(consumer, RwLock::new(mt.1)).await
    }));

    let user_balance_db = UserBalanceConfig {
        client,
        tablename: "user_balance".to_string(),
    };

    let cur_merkle_tree = Arc::new(RwLock::new(mt.0));
    let schema = Schema::build(QueryRoot, MutationRoot, EmptySubscription)
        .data(ApiContext {
            user_balance_db,
            mt: cur_merkle_tree.clone(),
            channel: channel.clone(),
        })
        .finish();
    let app = Router::new()
        .route("/", get(graphql_playground).post(graphql_handler))
        .route("/health", get(health))
        .layer(Extension(schema));
    handles.push(tokio::spawn(async move {
        Server::bind(&"0.0.0.0:8000".parse().unwrap())
            .serve(app.into_make_service())
            .await
            .unwrap()
    }));
    // }

    // if cli.mint {
    let provider = Arc::new(Provider::<Http>::try_from("https://rpc2.sepolia.org")?);

    let contract_address: H160 = ARCPAY_ADDRESS.parse::<Address>().unwrap();
    let contract = ArcPayContract::new(contract_address, provider);

    // let events = ;
    // .from_block(17915100) // TODO: save the last block processed.
    // .to_block(BlockNumber::Latest); // TODO: find the correct `to_block`.

    /*
    Here's another way of watching for events:
    let abi: Abi = serde_json::from_slice(include_bytes!("../ArcPay.json"))?;
    let contract = Contract::new(contract_address, abi, provider.clone());
    let event = contract.event_for_name("Mint")?;
    let filter = event.filter.from_block(0).to_block(BlockNumber::Finalized);
    */
    handles.push(tokio::spawn(async move {
        mint(contract, channel.clone(), cur_merkle_tree.clone()).await
    }));
    // }

    for handle in handles {
        handle.await.expect("joined task panicked");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use ethers::utils::hex;
    use eyre::Result;
    use pmtree::Hasher;
    use rln::circuit::Fr;
    use secp256k1::{Message, Secp256k1, SecretKey};
    use sha3::{Digest, Keccak256};

    use crate::{merkle::MyPoseidon, model::Leaf};
    // use super::*;
    #[test]
    fn generate_data() -> Result<()> {
        let secp = Secp256k1::new();
        let sk1 = SecretKey::from_str(
            "b4b0bf302506d14eba9970593921a0bd219a10ebf66a0367851a278f9a8c3d08",
        )?;
        let pk1 = sk1.public_key(&secp);
        dbg!(&pk1.serialize_uncompressed());
        let eth1_addr = hex::decode("8ca4cc18dc867aE7D87473f8460120168a895E7A")?;
        dbg!(&eth1_addr);
        assert_eq!(
            Keccak256::digest(&pk1.serialize_uncompressed()[1..65]).as_slice()[12..],
            eth1_addr
        );

        let sk2 = SecretKey::from_str(
            "b81676dc516f1e4dcec657669e30d31e4454e67aa98574eca670b4509878290c",
        )?;
        let pk2 = sk2.public_key(&secp);
        dbg!(&pk2.serialize_uncompressed());
        let eth2_addr = hex::decode("2C66bB06B88Bf3aB61aF23E70B0c8bE27b1e5930")?;
        dbg!(&eth2_addr);
        assert_eq!(
            Keccak256::digest(&pk2.serialize_uncompressed()[1..65]).as_slice()[12..],
            eth2_addr
        );

        let sk3 = SecretKey::from_str(
            "2a871d0798f97d79848a013d4936a73bf4cc922c825d33c1cf7073dff6d409c6",
        )?;
        let pk3 = sk3.public_key(&secp);
        dbg!(&pk3.serialize_uncompressed());
        let eth3_addr = hex::decode("a0Ee7A142d267C1f36714E4a8F75612F20a79720")?;
        dbg!(&eth3_addr);
        assert_eq!(
            Keccak256::digest(&pk3.serialize_uncompressed()[1..65]).as_slice()[12..],
            eth3_addr
        );

        let leaf = Leaf {
            address: eth1_addr.try_into().unwrap(),
            low_coin: 0,
            high_coin: 9,
        };

        let highest_coin_to_send: u64 = 5;
        let recipient: [u8; 20] = eth2_addr.try_into().unwrap();

        let mut receiver = vec![0u8; 12];
        receiver.extend_from_slice(&recipient);

        let msg = MyPoseidon::hash(&[
            Fr::from(leaf.low_coin),
            Fr::from(leaf.high_coin),
            Fr::from(highest_coin_to_send),
            MyPoseidon::deserialize(receiver.to_owned()),
        ]);

        let sig = secp.sign_ecdsa(
            &Message::from_slice(&MyPoseidon::serialize(msg)).unwrap(),
            &sk1,
        );
        dbg!(sig.serialize_compact());

        Ok(())
    }
}
