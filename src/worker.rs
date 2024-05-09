#![allow(dead_code)]
#![allow(unused_variables)]

extern crate asm4_2;
use asm4_2::map_reduce;
use rhai::{Engine, Scope};
use std::io::{BufRead, BufReader};

use std::collections::HashSet;
use std::env;
use std::io::Write;
use std::net::SocketAddr;
use std::ptr::addr_of;
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

static mut KEYS: Vec<String> = vec![];

#[derive(Debug, Default)]
struct WorkerService {
}

#[derive(Debug, Clone)]
struct Record {
    key: String,
    value: String,
}

impl Record {
    fn new(str: String) -> Record {
        Record {
            key: str.split(',').nth(0).unwrap().to_string(),
            value: str.split(',').nth(1).unwrap().to_string(),
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
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let mut engine = Engine::new();
        let script = payload.map_func;

        // // register the emit function
        // let ast = engine.compile(script).unwrap();
        // engine.register_fn("emit", |key: String, value: String, dir_path: String| {
        //     unsafe {
        //         if !KEYS.contains(&key) {
        //             KEYS.push(key.clone());
        //         }
        //     }
        //     fn open_file(key: String, dir_path: String) -> std::fs::File {
        //         let path = format!("{}/{}.kv", dir_path, key);
        //         std::fs::OpenOptions::new().create(true).write(true).append(true).open(path).unwrap()
        //     }
        //     let mut file = open_file(key.clone(), dir_path.clone());
        //     let out = format!("{},{}\n", key.clone(), value.clone());
        //     file.write(out.as_bytes()).unwrap();
        // });
        //
        // // assign symbols to scope
        // let mut scope = Scope::new();
        // scope.push("dir_path", "data/intermediate/");
        // unsafe {scope.push("keys", addr_of!(KEYS))};
        //
        // let file = std::fs::File::open(payload.input_filepath)?;
        // let reader = BufReader::new(file);
        // for (index, line) in reader.lines().enumerate() {
        //     let line = line?;
        //     let record = Record::new(line);
        //     let _: () = engine.call_fn(&mut scope, &ast, "map", (record.key, record.value)).unwrap();
        // }

        let mut keys: Vec<String> = vec![];
        let dir_path = "data/intermediate";

        // Emitter for map
        let mut emit = |key: String, value: String| {
            fn open_file(keys: &mut Vec<String>, key: String, dir_path: &str) -> std::fs::File {
                if !keys.contains(&key) {
                    keys.push(key.clone());
                }
                let path = format!("{}/{}.kv", dir_path, key.clone());
                std::fs::OpenOptions::new().create(true).write(true).append(true).open(path).unwrap()
            }
            let mut file = open_file(&mut keys, key.clone(), dir_path);
            let out = format!("{},{}\n", key.clone(), value.clone());
            file.write(out.as_bytes()).unwrap();
        };

        // Define map function
        let mut map = |key: String, value: String| {
            emit(key, value);
        };

        let file = std::fs::File::open(payload.input_filepath)?;
        let reader = BufReader::new(file);
        let mut n_records_processed = 0;
        for (index, line) in reader.lines().enumerate() {
            let line = line.unwrap();
            let record = Record::new(line);
            map(record.key, record.value);
            n_records_processed += 1;
        }

        // Send response back
        let mut resp = MapJobStatusResponse::default();
        resp.keys = keys;
        resp.n_records_processed = Some(n_records_processed);
 
        tokio::spawn(async move {
            tx.send(Ok(resp)).await.unwrap();
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type StartReduceJobStream = ReceiverStream<Result<ReduceJobStatusResponse, Status>>;

    async fn start_reduce_job(
        &self,
        request: Request<ReduceJobRequest>,
    ) -> Result<Response<Self::StartReduceJobStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let payload = request.into_inner();

        fn emit (key: String, value: String, out_file: String) {
            fn open_file(out_file: String) -> std::fs::File {
                let path = format!("{}.kv", out_file);
                std::fs::OpenOptions::new().create(true).write(true).append(true).open(path).unwrap()
            }
            let mut file = open_file(out_file);
            let out = format!("{},{}\n", key, value);
            file.write(out.as_bytes()).unwrap();
        }

        fn reduce(key: String, values: Vec<String>, out_file: String) {
            let mut total = 0;
            for v in values {
                let n = v.parse::<i32>().unwrap();
                total += n;
            }
            emit(key, total.to_string(), out_file);
        }

        let file = std::fs::File::open(payload.input_fiilepath.clone()).unwrap();
        let reader = BufReader::new(file);
        let mut values: Vec<String> = vec![];
        let mut key = String::new();
        let mut n_records_processed = 0;
        for (index, line) in reader.lines().enumerate() {
            let line = line.unwrap();
            let record = Record::new(line);
            if key == String::new() {
                key = record.key;
            } else if key != record.key {
                let status = tonic::Status::new(tonic::Code::InvalidArgument, format!("Invalid key in file. Line: {}", index));
                return Err(status);
            }
            values.push(record.value);
            n_records_processed += 1;
        }
        reduce(key, values, payload.output_filepath.clone());
        
        // Send response back
        let mut resp = ReduceJobStatusResponse::default();
        resp.status = JobStatus::Exiting.into();
        resp.n_records_processed = Some(n_records_processed);
 
        tokio::spawn(async move {
            tx.send(Ok(resp)).await.unwrap();
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }


    // DEPRECATED:
    // Shuffle is not needed
   
    type StartShuffleJobStream = ReceiverStream<Result<ShuffleJobStatusResponse, Status>>;

    async fn start_shuffle_job(
        &self,
        request: tonic::Request<ShuffleJobRequest>,
    ) -> Result<Response<Self::StartShuffleJobStream>, Status> {
        unimplemented!("DEPRECATED: Shuffle is not needed")
    }
}

impl WorkerService {
    async fn get_or_create_file(path: String, files: HashSet<String>) {
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
            let mut stream = conn.start_map_job(job).await.unwrap().into_inner();
            let mut result = MapJobStatusResponse::default();
            while let Some(resp) = stream.message().await? {
                result = resp;
            }
            println!("{:?}", result);
            return Ok(result);
        }
        Err("Connection is None".into())
    }

    pub async fn assign_reduce_job(
        &mut self,
        job: ReduceJobRequest
    ) -> Result<ReduceJobStatusResponse, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(conn) = &mut self.conn {
            let mut stream = conn.start_reduce_job(job).await.unwrap().into_inner();
            let mut result = ReduceJobStatusResponse::default();
            while let Some(resp) = stream.message().await.unwrap() {
                result = resp;
            }
            println!("ReduceJobStatusResponse: {:?}", result);
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
