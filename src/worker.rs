#![allow(dead_code)]
#![allow(unused_variables)]

extern crate asm4_2;
use asm4_2::map_reduce;

use std::env;
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use serde::Deserialize;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

use map_reduce::worker_client::WorkerClient;
use map_reduce::worker_server::{Worker, WorkerServer};
use map_reduce::JobStatus;
use map_reduce::{MapJobRequest, ReduceJobRequest, ShuffleJobRequest};
use map_reduce::{MapJobStatusResponse, ReduceJobStatusResponse, ShuffleJobStatusResponse};

#[derive(Debug, Default)]
struct WorkerService {}

#[derive(Debug, Clone)]
struct Record {
    key: String,
    value: String,
}

impl Record {
    fn new(str: String) -> Record {
        Record {
            key: str.split('\t').next().unwrap().to_string(),
            value: str.split('\t').nth(1).unwrap().to_string(),
        }
    }
}

#[tonic::async_trait]
impl Worker for WorkerService {
    type StartMapJobStream = ReceiverStream<Result<MapJobStatusResponse, Status>>;

    async fn start_map_job(
        &self,
        request: Request<MapJobRequest>,
    ) -> Result<Response<Self::StartMapJobStream>, Status> {
        let payload = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        // let responses = vec![
        //     MapJobStatusResponse {
        //         job_id: payload.job_id.clone(),
        //         status: JobStatus::Mapping.into(),
        //         ..Default::default()
        //     },
        //     MapJobStatusResponse {
        //         job_id: payload.job_id.clone(),
        //         status: JobStatus::Mapping.into(),
        //         ..Default::default()
        //     },
        //     MapJobStatusResponse {
        //         job_id: payload.job_id.clone(),
        //         status: JobStatus::Mapping.into(),
        //         ..Default::default()
        //     },
        //     MapJobStatusResponse {
        //         job_id: payload.job_id.clone(),
        //         status: JobStatus::Mapping.into(),
        //         ..Default::default()
        //     },
        // ];
        //
        // for resp in responses {
        //     if let Err(e) = tx.send(Ok(resp)).await {
        //         eprintln!("Error: {:?}", e);
        //     } else {
        //         println!("Sent response");
        //     }
        //     tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        // }

        let file = std::fs::File::open(payload.input_filepath)?;
        let reader = BufReader::new(file);
        for (index, line) in reader.lines().enumerate() {
            let line = line?;
            let record = Record::new(line);
            // payload.
        }

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type StartReduceJobStream = ReceiverStream<Result<ReduceJobStatusResponse, Status>>;

    async fn start_reduce_job(
        &self,
        request: Request<ReduceJobRequest>,
    ) -> Result<Response<Self::StartReduceJobStream>, Status> {
        println!("{:?}", request.into_inner());
        todo!()
    }

    type StartShuffleJobStream = ReceiverStream<Result<ShuffleJobStatusResponse, Status>>;

    async fn start_shuffle_job(
        &self,
        request: tonic::Request<ShuffleJobRequest>,
    ) -> Result<Response<Self::StartShuffleJobStream>, Status> {
        println!("Shuffle started");

        let payload = request.into_inner();
        let key_filepaths = payload.key_filepaths.unwrap();
        let mut output = std::fs::File::open(key_filepaths.key)?;
        let mut bytes_written = 0;
        let n_files = key_filepaths.filepaths.len();
        for path in key_filepaths.filepaths {
            let mut file = std::fs::File::open(path)?;
            bytes_written += std::io::copy(&mut file, &mut output)?;
        }

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.send(Ok(ShuffleJobStatusResponse {
            job_id: payload.job_id,
            status: JobStatus::Exiting as i32,
            n_files_processed: Some(n_files as u32),
            n_files_failed: Some(0),
            percentage_complete: Some(100),
        }))
        .await
        .unwrap();
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[derive(Debug, Default, Deserialize)]
struct WorkersConfig {
    workers: Vec<WorkerConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct WorkerConfig {
    id: String,
    ip: String,
    port: u32,
}

impl WorkersConfig {
    pub fn from_yaml(
        yaml: &String,
    ) -> Result<WorkersConfig, Box<dyn std::error::Error + Send + Sync>> {
        let config: WorkersConfig = serde_yaml::from_str(&yaml)?;
        Ok(config)
    }
}

impl WorkerConfig {
    fn addr(&self) -> SocketAddr {
        SocketAddr::from_str(&format!("{}:{}", self.ip, self.port)).unwrap()
    }
}

#[derive(Debug)]
pub struct WorkerPool {
    workers: Vec<Mutex<WorkerNode>>,
    status_rx: tokio::sync::mpsc::Receiver<UpdateWorkerStatusEvent>, // (worker_id, status)
    status_tx: tokio::sync::mpsc::Sender<UpdateWorkerStatusEvent>,   // (worker_id, status)
}

#[derive(Debug)]
struct UpdateWorkerStatusEvent {
    worker_id: String,
    status: WorkerStatus,
}

#[derive(Debug, Clone)]
pub struct WorkerNode {
    id: String,
    addr: SocketAddr,
    status: WorkerStatus,
    conn: Option<WorkerClient<Channel>>,
}

impl WorkerNode {
    ///
    /// Assign a map job to the worker
    ///
    /// Does not return until the job is completed
    ///
    pub async fn assign_map_job(
        &mut self,
        job: MapJobRequest,
    ) -> Result<MapJobStatusResponse, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(conn) = &mut self.conn {
            let mut stream = conn.start_map_job(job).await?.into_inner();
            let mut result = MapJobStatusResponse::default();
            while let Some(resp) = stream.message().await? {
                println!("assign_map_job: {:?}", resp);
                result = resp;
            }
            return Ok(result);
        }
        Err("Connection is None".into())
    }

    fn is_free(&self) -> bool {
        match self.status {
            WorkerStatus::Free => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
enum WorkerStatus {
    Busy,
    Free,
    Disconnected,
    Dead,
}

impl WorkerPool {
    pub async fn get_free_worker(&self) -> Option<&Mutex<WorkerNode>> {
        loop {
            for i in 0..self.workers.len() {
                if let Some(w) = self.workers[i].try_lock().ok() {
                    if w.is_free() {
                        return Some(&self.workers[i]);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    pub async fn from_yaml(
        filepath: &String,
    ) -> Result<WorkerPool, Box<dyn std::error::Error + Send + Sync>> {
        let yaml_config = std::fs::read_to_string(&filepath)?;
        let workers_config = WorkersConfig::from_yaml(&yaml_config)?;
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let mut pool = WorkerPool {
            status_rx: rx,
            status_tx: tx,
            workers: Vec::new(),
        };
        for conf in workers_config.workers {
            let conn = WorkerClient::connect(format!("http://{}", conf.addr())).await?;
            pool.workers.push(Mutex::new(WorkerNode {
                id: conf.id.clone(),
                addr: conf.addr(),
                status: WorkerStatus::Free,
                conn: Some(conn),
            }))
        }

        Ok(pool)
    }

    pub async fn assign_map_job(
        &self,
        worker: &mut WorkerNode,
        job: MapJobRequest,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        worker.assign_map_job(job).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let listen_port: u32 = args[1].parse().unwrap();
    let addr = std::net::SocketAddr::from_str(format!("127.0.0.1:{}", listen_port).as_str())?;
    let svc = WorkerService::default();
    Server::builder()
        .add_service(WorkerServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}
