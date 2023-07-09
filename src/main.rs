use std::env;

use async_graphql::{Schema, EmptySubscription};
use futures_lite::stream::StreamExt;

use axum::{extract::Extension, routing::get, Router, Server};
use lapin::publisher_confirm::Confirmation;
use pg_bigdecimal::PgNumeric;
use tokio_postgres::NoTls;

use std::sync::Arc;
use futures::lock::Mutex;
use model::MutationRoot;

use lapin::{
    options::*, types::FieldTable, BasicProperties, Connection, ConnectionProperties,
};

use crate::{merkle::{PostgresDBConfig, get_new_merkle_tree, MyPoseidon}, routes::{graphql_playground, graphql_handler, health}, model::QueryRoot};
use pmtree::MerkleTree;
mod model;
mod routes;
mod merkle;

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

#[tokio::main]
async fn main() {
    //////////////// experimental RabbitMQ integration ////////////////
    // Connect to the RabbitMQ server
    let addr = "amqp://guest:guest@localhost:5672/%2f";
    let conn = Connection::connect(addr, ConnectionProperties::default()).await.unwrap();

    // Create a channel
    let channel = conn.create_channel().await.unwrap();

    // Declare a queue
    let queue_name = "send_request_queue";
    channel
        .queue_declare(queue_name, QueueDeclareOptions::default(), FieldTable::default())
        .await.unwrap();

    // Consume messages from the queue
    let mut consumer = channel
        .basic_consume(
            queue_name,
            "my_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await.unwrap();

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
    ///////////////////////////////////////////////////////////////////

    //////////////// experimental GraphQL integration /////////////////
    let (client, connection) = tokio_postgres::connect("host=localhost user=dev dbname=arcpay", NoTls).await.unwrap();

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let args: Vec<String> = env::args().collect();

    let mt = match args[1].as_str() {
        "new" => get_new_merkle_tree(32, client, "test".to_string()).await,
        "load" => MerkleTree::<PostgresDBConfig, MyPoseidon>::load(PostgresDBConfig {
            client,
            tablename: "test".to_string()
        }).await.unwrap(),
        _ => panic!("only new or load")
    };
    let schema = Schema::build(QueryRoot, MutationRoot, EmptySubscription)
        .data(Arc::new(Mutex::new(mt)))
        .finish();
    let app = Router::new()
        .route("/", get(graphql_playground).post(graphql_handler))
        .route("/health", get(health))
        .layer(Extension(schema));
    Server::bind(&"0.0.0.0:8000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
