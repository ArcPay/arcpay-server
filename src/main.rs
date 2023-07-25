use async_graphql::{EmptySubscription, Schema};
use ethers_providers::{Http, Middleware, Provider};

use axum::{extract::Extension, routing::get, Router, Server};
use lapin::Channel;
use tokio_postgres::NoTls;
use user_balance::UserBalanceConfig;

use model::MutationRoot;
use std::sync::Arc;
use tokio::sync::RwLock;

use lapin::{options::*, types::FieldTable, Connection, ConnectionProperties};

use clap::Parser;

use crate::{
    merkle::{MyPoseidon, PostgresDBConfig},
    model::QueryRoot,
    routes::{graphql_handler, graphql_playground, health},
};
use pmtree::MerkleTree;
mod merkle;
mod model;
mod routes;
mod send_consumer;
mod user_balance;

#[derive(Parser, Debug)]
struct Cli {
    /// Create or load a merkle tree
    #[arg(long)]
    merkle: Option<MerkleCommand>,
}

#[derive(clap::ValueEnum, Debug, Clone)]
enum MerkleCommand {
    New,
    Load,
}

struct ApiContext {
    user_balance_db: UserBalanceConfig,
    mt: Arc<RwLock<MerkleTree<PostgresDBConfig, MyPoseidon>>>,
    channel: Channel,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    //////////////// experimental RabbitMQ integration ////////////////
    // Connect to the RabbitMQ server
    let addr = "amqp://guest:guest@localhost:5672/%2f";
    let conn = Connection::connect(addr, ConnectionProperties::default()).await?;

    // Create a channel
    let channel = conn.create_channel().await?;

    // Declare a queue
    let queue_name = "send_request_queue";

    // TODO: declare queue as persistent through `QueueDeclareOptions`. See message durability section:
    // https://www.rabbitmq.com/tutorials/tutorial-two-python.html.
    // Also make messages persistent by updating publisher. The value `PERSISTENT_DELIVERY_MODE` is 2,
    // see: https://pika.readthedocs.io/en/stable/_modules/pika/spec.html.
    // Even this doesn't fully guarantee message persistence, see "Note on message persistence"
    // in the tutorial which links to: https://www.rabbitmq.com/confirms.html#publisher-confirms.
    // We have enabled publisher confirms in the next step.
    channel
        .queue_declare(
            queue_name,
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
            queue_name,
            "my_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("basic_consume");

    println!("Waiting for messages...");

    //////////////// experimental GraphQL integration /////////////////
    if let Some(t) = cli.merkle {
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

        tokio::spawn(async move { send_consumer::send_consumer(consumer, t).await });
        let provider = Provider::<Http>::try_from("https://eth.llamarpc.com")?;
        let block = provider.get_block(100u64).await?;
        println!("Got block: {}", serde_json::to_string(&block)?);

        let user_balance_db = UserBalanceConfig {
            client,
            tablename: "user_balance".to_string(),
        };

        let schema = Schema::build(QueryRoot, MutationRoot, EmptySubscription)
            .data(ApiContext {
                user_balance_db,
                mt: Arc::new(RwLock::new(mt)),
                channel,
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

    Ok(())
}
