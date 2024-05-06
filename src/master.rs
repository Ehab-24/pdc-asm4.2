#![allow(dead_code)]
#![allow(unused_variables)]

extern crate asm4_2;
use asm4_2::map_reduce;

mod shuffler;
mod gate_keeper;
mod worker;

use ::futures::future::join_all;
use map_reduce::{JobConfig, MapJobRequest};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;

use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Response, Status};

use map_reduce::{
    client_server::{Client, ClientServer},
    JobStatus, JobStatusResponse, SubmitJobRequest,
};
use uuid::Uuid;
use worker::WorkerPool;

#[derive(Default)]
struct ClientService {}

#[tonic::async_trait]
impl Client for ClientService {
    type SubmitJobStream = ReceiverStream<Result<JobStatusResponse, Status>>;

    async fn submit_job(
        &self,
        request: tonic::Request<SubmitJobRequest>,
    ) -> Result<Response<Self::SubmitJobStream>, Status> {
        let payload = request.into_inner();
        let (tx, rx) = mpsc::channel(4);
        if let Err(e) = tokio::spawn(async move {
            if let Err(e) = Master::run(&tx, payload).await {
                eprintln!("Error: {:?}", e);
            }
        })
        .await
        {
            eprintln!("Error: {}", e)
        }

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[derive(Debug)]
struct Master {}

impl Master {
    async fn run(
        tx: &mpsc::Sender<Result<JobStatusResponse, Status>>,
        request: SubmitJobRequest,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let job_id = Uuid::new_v4().to_string();
        ClientService::send_pending_message(tx, job_id.clone(), "initializing worker pool").await;

        let worker_pool = Arc::new(RwLock::new(
            WorkerPool::from_yaml(&"res/workers.yaml".to_string()).await?,
        ));
        // let worker_pool = WorkerPool::from_yaml(&"res/workers.yaml".to_string()).await?;

        ClientService::send_started_message(&tx, job_id.clone(), "").await;

        let map_jobs = if let Some(config) = request.job_config {
            create_map_jobs(&job_id, config)
        } else {
            ClientService::send_failure_response(&tx, job_id.clone(), "No job config provided")
                .await;
            return Ok(());
        };

        let futs: Vec<_> = map_jobs
            .into_iter()
            .map(|job| {
                let worker_pool = Arc::clone(&worker_pool);
                async move {
                    if let Some(w) = worker_pool.read().await.get_free_worker().await {
                        let mut w = w.lock().await;
                        let response = w.assign_map_job(job).await.unwrap();
                        println!("Job completed");
                    }
                }
            })
            .collect();
        let some = join_all(futs).await;

        // create shuffle jobs
        let shuffle_jobs = 

        // assign shuffle jobs to workers

        Ok(())
    }
}

impl ClientService {
    #[allow(dead_code)]
    async fn send_pending_message(
        tx: &mpsc::Sender<Result<JobStatusResponse, Status>>,
        job_id: String,
        message: &str,
    ) {
        ClientService::send_message(tx, job_id, message, JobStatus::Pending).await;
    }

    async fn send_started_message(
        tx: &mpsc::Sender<Result<JobStatusResponse, Status>>,
        job_id: String,
        message: &str,
    ) {
        ClientService::send_message(tx, job_id, message, JobStatus::Started).await;
    }

    async fn send_failure_response(
        tx: &mpsc::Sender<Result<JobStatusResponse, Status>>,
        job_id: String,
        message: &str,
    ) {
        ClientService::send_message(tx, job_id, message, JobStatus::Terminating).await;
    }

    async fn send_message(
        tx: &mpsc::Sender<Result<JobStatusResponse, Status>>,
        job_id: String,
        message: &str,
        status: JobStatus,
    ) {
        let response = JobStatusResponse {
            job_id: job_id.clone(),
            status: status.into(),
            detail: message.to_string(),
            ..Default::default()
        };
        tx.send(Ok(response)).await.unwrap();
    }
}

fn create_map_jobs(job_id: &String, config: JobConfig) -> Vec<MapJobRequest> {
    config
        .input_files
        .iter()
        .map(|file| {
            let map_job = MapJobRequest {
                job_id: job_id.clone(),
                output_directory: config.output_directory.clone(),
                input_filepath: file.to_string(),
            };
            map_job
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::net::SocketAddr::from_str("[::1]:50051")?;
    let svc = ClientService::default();
    Server::builder()
        .add_service(ClientServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}
