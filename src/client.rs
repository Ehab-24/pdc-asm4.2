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

    let mut input_files = vec![];
    for i in 0..10 {
        input_files.push(format!("file{}.txt", i));
    }
    let output_directory = "output".to_string();
    let num_reducers = 2;
    let map_func = "map_func".to_string();
    let reduce_func = "reduce_func".to_string();

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
