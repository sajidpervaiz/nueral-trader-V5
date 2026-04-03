use std::env;
use std::net::SocketAddr;

use gateway::TradingGateway;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = env::var("GATEWAY_GRPC_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = env::var("GATEWAY_GRPC_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(50051);

    let addr: SocketAddr = format!("{}:{}", host, port).parse()?;
    let gateway = TradingGateway::new();

    let processor = gateway.run_message_processor();
    tokio::spawn(processor);

    gateway.serve_grpc(addr).await
}
