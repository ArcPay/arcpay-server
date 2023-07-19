use async_graphql::{Schema, EmptySubscription};
use ethers_providers::{Provider, Http, Middleware};
use futures_lite::stream::StreamExt;

use axum::{extract::Extension, routing::get, Router, Server};
use lapin::publisher_confirm::Confirmation;
use tokio_postgres::NoTls;
use user_balance::UserBalanceConfig;

use std::sync::Arc;
use tokio::sync::RwLock;
use model::MutationRoot;

use lapin::{
    options::*, types::FieldTable, BasicProperties, Connection, ConnectionProperties,
};

use clap::Parser;

use crate::{merkle::{PostgresDBConfig, MyPoseidon}, routes::{graphql_playground, graphql_handler, health}, model::QueryRoot};
use pmtree::MerkleTree;
mod model;
mod routes;
mod merkle;
mod user_balance;

#[derive(Parser, Debug)]
struct Cli {
    /// Connect to a rabbitmq instance
    #[arg(long, action=clap::ArgAction::SetTrue)]
    rabbitmq: bool,

    /// Create or load a merkle tree
    #[arg(long)]
    merkle: Option<MerkleCommand>,
}

#[derive(clap::ValueEnum, Debug, Clone)]
enum MerkleCommand {
    New,
    Load
}

struct DBContext {
    user_balance_db: UserBalanceConfig,
    mt: Arc<RwLock<MerkleTree<PostgresDBConfig, MyPoseidon>>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    //////////////// experimental RabbitMQ integration ////////////////
    if cli.rabbitmq {
        // Connect to the RabbitMQ server
        let addr = "amqp://guest:guest@localhost:5672/%2f";
        let conn = Connection::connect(addr, ConnectionProperties::default()).await?;

        // Create a channel
        let channel = conn.create_channel().await?;

        // Declare a queue
        let queue_name = "send_request_queue";
        channel
            .queue_declare(queue_name, QueueDeclareOptions::default(), FieldTable::default())
            .await?;

        // Consume messages from the queue
        let mut consumer = channel
            .basic_consume(
                queue_name,
                "my_consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        println!("Waiting for messages...");

        // Process incoming messages in a separate thread.
        tokio::spawn(async move {
            while let Some(delivery) = consumer.next().await {
                let delivery = delivery.expect("error in consumer");
                delivery
                    .ack(BasicAckOptions::default())
                    .await
                    .expect("ack");
                // dbg!(delivery); // uncomment if you want to see continuous dump.
            }
        });

        // Publish messages to the queue in a separate thread.
        let message = b"Hello, RabbitMQ!";
        tokio::spawn(async move {
            loop {
                let confirm = channel
                    .basic_publish(
                        "",
                        queue_name,
                        BasicPublishOptions::default(),
                        message,
                        BasicProperties::default(),
                    )
                    .await.unwrap().await.unwrap();
                assert_eq!(confirm, Confirmation::NotRequested);
            }
        });
    }
    ///////////////////////////////////////////////////////////////////

    //////////////// experimental GraphQL integration /////////////////
    match cli.merkle {
        Some(t) => {
            let (client, connection) = tokio_postgres::connect("host=localhost user=dev dbname=arcpay", NoTls).await?;

            // The connection object performs the actual communication with the database,
            // so spawn it off to run on its own.
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });

            let client = Arc::new(RwLock::new(client));

            let mt = match t {
                MerkleCommand::New => MerkleTree::<PostgresDBConfig, MyPoseidon>::new(
                    32,
                    PostgresDBConfig {
                        client: client.clone(),
                        tablename: "test".to_string()
                    }).await?,
                MerkleCommand::Load => MerkleTree::<PostgresDBConfig, MyPoseidon>::load(
                    PostgresDBConfig {
                        client: client.clone(),
                        tablename: "test".to_string()
                    }).await?,
            };

            let user_balance_db = UserBalanceConfig {
                client,
                tablename: "user_balance".to_string()
            };

            let schema = Schema::build(QueryRoot, MutationRoot, EmptySubscription)
                .data(DBContext {
                    user_balance_db,
                    mt: Arc::new(RwLock::new(mt))
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

        None => {}
    }

    let provider = Provider::<Http>::try_from("https://eth.llamarpc.com")?;
    let block = provider.get_block(100u64).await?;
    println!("Got block: {}", serde_json::to_string(&block)?);
    Ok(())
}
