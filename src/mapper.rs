pub mod map_reduce {
    tonic::include_proto!("mapreduce");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, World!");
    Ok(())
}
