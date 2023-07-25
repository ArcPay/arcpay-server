use async_graphql::{EmptySubscription, Schema};
use ethers::prelude::*;

use axum::{extract::Extension, routing::get, Router, Server};
use lapin::Channel;
use serde::{Deserialize, Serialize};
use tokio_postgres::NoTls;
use user_balance::UserBalanceConfig;

use model::{MutationRoot, SendMessageType};
use std::sync::Arc;
use tokio::sync::RwLock;

use lapin::{options::*, types::FieldTable, Connection, ConnectionProperties};

use clap::Parser;

use crate::{
    merkle::{MyPoseidon, PostgresDBConfig},
    mint::mint,
    model::QueryRoot,
    routes::{graphql_handler, graphql_playground, health},
};
use pmtree::MerkleTree;
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
    merkle: Option<MerkleCommand>,
    /// Activates the mint pipeline
    #[arg(long, action=clap::ArgAction::SetTrue)]
    mint: bool,
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
const ARCPAY_ADDRESS: &str = "0xCf7Ed3AccA5a467e9e704C703E8D87F634fB0Fc9";

const QUEUE_NAME: &str = "user_requests";

abigen!(ArcPayContract, "ArcPay.json");

#[derive(Debug, Serialize, Deserialize)]
enum QueueMessage {
    Mint { receiver: H160, amount: U256 },
    Send(SendMessageType),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
    tokio::spawn(async move { send_consumer::send_consumer(consumer).await });

    let cli = Cli::parse();
    let channel = Arc::new(channel);

    if let Some(t) = cli.merkle {
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
            merkle_table: "test".to_string(),
            pre_image_table: "pre_image".to_string(),
        };

        let mt = match t {
            MerkleCommand::New => {
                MerkleTree::<PostgresDBConfig, MyPoseidon>::new(32, db_config).await?
            }
            MerkleCommand::Load => {
                MerkleTree::<PostgresDBConfig, MyPoseidon>::load(db_config).await?
            }
        };

        let user_balance_db = UserBalanceConfig {
            client,
            tablename: "user_balance".to_string(),
        };

        let schema = Schema::build(QueryRoot, MutationRoot, EmptySubscription)
            .data(ApiContext {
                user_balance_db,
                mt: Arc::new(RwLock::new(mt)),
                channel: channel.clone(),
            })
            .finish();
        let app = Router::new()
            .route("/", get(graphql_playground).post(graphql_handler))
            .route("/health", get(health))
            .layer(Extension(schema));
        Server::bind(&"0.0.0.0:8000".parse()?)
            .serve(app.into_make_service())
            .await?;
    }

    if cli.mint {
        let provider = Arc::new(Provider::<Http>::try_from("http://127.0.0.1:8545")?);
        let contract_address = ARCPAY_ADDRESS.parse::<Address>()?;

        let contract = ArcPayContract::new(contract_address, provider);
        let events = contract
            .event::<MintFilter>()
            .from_block(0)
            .to_block(BlockNumber::Finalized); // TODO: find the correct `to_block`.

        /*
        Here's another way of watching for events:
        let abi: Abi = serde_json::from_slice(include_bytes!("../ArcPay.json"))?;
        let contract = Contract::new(contract_address, abi, provider.clone());
        let event = contract.event_for_name("Mint")?;
        let filter = event.filter.from_block(0).to_block(BlockNumber::Finalized);
        */
        let mint_task = tokio::spawn(mint(events, channel.clone()));
        mint_task.await?;
    }

    Ok(())
}
