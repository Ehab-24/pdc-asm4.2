use std::env;

use tonic::Request;

use map_reduce::client_client::ClientClient;
use map_reduce::{JobConfig, SubmitJobRequest};

pub mod map_reduce {
    tonic::include_proto!("mapreduce");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let args: Vec<String> = env::args().collect();
    let listen_port: i32 = args[1].parse().unwrap();

    let master_endpoint = format!("http://[::1]:{}", listen_port);
    let mut client = ClientClient::connect(master_endpoint).await?;

    let input_files = vec![
        String::from("data/input/input1.txt"),
        String::from("data/input/input2.txt"),
        String::from("data/input/input3.txt"),
        String::from("data/input/input4.txt"),
        String::from("data/input/input5.txt"),
    ];
    let output_directory = "data/results".to_string();
    let num_reducers = 2;
    let map_func = String::new();
    let reduce_func = String::new();

    let config = JobConfig {
        input_files,
        output_directory,
        num_reducers,
        map_func,
        reduce_func,
    };

    let payload = SubmitJobRequest {
        job_config: Some(config),
    };
    let mut stream = client.submit_job(Request::new(payload)).await?.into_inner();
    while let Some(message) = stream.message().await? {
        println!("Job Status Response: {:?}", message)
    }
    Ok(())
}
