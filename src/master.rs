#![allow(dead_code)]
#![allow(unused_variables)]

extern crate asm4_2;
use asm4_2::map_reduce;

mod shuffler;
mod gate_keeper;
mod worker;

use asm4_2::map_reduce::KeyFilepaths;
use asm4_2::map_reduce::ReduceJobRequest;
use asm4_2::map_reduce::ShuffleJobRequest;
use ::futures::future::join_all;

use map_reduce::{JobConfig, MapJobRequest};
use std::collections::HashSet;
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

        fn data_dir(path: &str) -> String {
            format!("data/{}", path)
        }

        ClientService::send_started_message(&tx, job_id.clone(), "").await;

        let map_jobs = if let Some(config) = request.job_config.clone() {
            create_map_jobs(job_id.clone(), config, data_dir("map"))
        } else {
            ClientService::send_failure_response(&tx, job_id.clone(), "No job config provided")
                .await;
            return Ok(());
        };

        let map_keys = Arc::new(RwLock::new(HashSet::new()));
        let futs: Vec<_> = map_jobs
            .into_iter()
            .map(|job| {
                let worker_pool = Arc::clone(&worker_pool);
                let map_keys = Arc::clone(&map_keys);
                async move {
                    if let Some(w) = worker_pool.read().await.get_free_worker().await {
                        let mut w = w.lock().await;
                        let response = w.assign_map_job(job).await.unwrap();
                        let some = response.keys.clone();
                        for k in some {
                            map_keys.write().await.insert(k);
                        }
                    }
                }
            })
            .collect();

        let _ = join_all(futs).await;
        println!("Map Keys: {:?}", map_keys.read().await);

        // ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ Reduce ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ 
        
        let reduce_jobs = if let Some(config) = request.job_config.clone() {
            let map_keys = map_keys.read().await;
            let map_keys = map_keys.clone().into_iter().collect();
            let intermediate_dir = data_dir("intermediate");
            let out_dir = config.output_directory.clone();
            create_reduce_jobs(job_id, config, map_keys, intermediate_dir, out_dir)
        } else {
            ClientService::send_failure_response(&tx, job_id.clone(), "Invalid job config")
                .await;
            return Ok(());
        };

        let futs: Vec<_> = reduce_jobs.into_iter()
            .map(|job| {
                let worker_pool = Arc::clone(&worker_pool);
                async move {
                    if let Some(w) = worker_pool.read().await.get_free_worker().await {
                        let mut w = w.lock().await;
                        let response = w.assign_reduce_job(job).await.unwrap();
                    }
                }
            })
                .collect();

        let _ = join_all(futs).await;

        Ok(())
    }
}

impl ClientService {
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

fn create_map_jobs(job_id: String, config: JobConfig, out_dir: String) -> Vec<MapJobRequest> {
    config
        .input_files
        .iter()
        .map(|file| {
            let map_job = MapJobRequest {
                job_id: job_id.clone(),
                output_directory: out_dir.clone(),
                input_filepath: file.to_string(),
                map_func: config.map_func.clone(),
            };
            map_job
        })
        .collect()
}

fn create_reduce_jobs(job_id: String, config: JobConfig, keys: Vec<String>, intermediate_dir: String, out_dir: String) -> Vec<ReduceJobRequest> {
    keys.iter().map(|k| {
        ReduceJobRequest {
            input_fiilepath: format!("{}/{}.kv", intermediate_dir, k),
            output_filepath: format!("{}/{}", out_dir, k),
            job_id: job_id.clone(),
            reduce_func: config.reduce_func.clone()
        }
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
