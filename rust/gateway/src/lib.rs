use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver, Sender};
use log::{debug, error, info};
use prometheus::{Counter, Encoder, Gauge, Histogram, HistogramOpts, TextEncoder};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tonic::{transport::Server, Request, Response, Status};

pub mod proto {
    tonic::include_proto!("neural_trader.bridge");
}

use proto::bridge_service_server::{BridgeService, BridgeServiceServer};
use proto::{
    BridgeConfig, HealthCheckRequest, HealthCheckResponse, MarketDataRequest, MarketDataResponse,
    MetricsRequest, MetricsResponse, OrderRequest, OrderResponse, PingRequest, PingResponse,
    SetConfigRequest, SetConfigResponse, SubscribeRequest, SubscribeResponse,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayMessage {
    pub timestamp_ns: i64,
    pub message_type: String,
    pub payload: Vec<u8>,
    pub correlation_id: String,
}

#[derive(Debug, Clone)]
pub struct Metrics {
    requests_total: Counter,
    requests_duration: Histogram,
    active_connections: Gauge,
    orders_received: Counter,
    orders_processed: Counter,
    market_data_received: Counter,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            requests_total: Counter::new("gateway_requests_total", "Total number of requests").unwrap(),
            requests_duration: Histogram::with_opts(
                HistogramOpts::new("gateway_requests_duration_seconds", "Request duration"),
            )
            .unwrap(),
            active_connections: Gauge::new("gateway_active_connections", "Active connections").unwrap(),
            orders_received: Counter::new("gateway_orders_received_total", "Total orders received").unwrap(),
            orders_processed: Counter::new("gateway_orders_processed_total", "Total orders processed").unwrap(),
            market_data_received: Counter::new(
                "gateway_market_data_received_total",
                "Total market data messages",
            )
            .unwrap(),
        }
    }

    pub fn export(&self) -> String {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        let metric_families = prometheus::gather();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap_or_default()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Default)]
pub struct OrderRouter {
    pending_orders: Arc<RwLock<HashMap<String, OrderRequest>>>,
    order_responses: Arc<RwLock<HashMap<String, OrderResponse>>>,
}

impl OrderRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn submit_order(&self, order: OrderRequest) -> Result<String, Status> {
        let order_id = format!(
            "ord-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        self.pending_orders.write().await.insert(order_id.clone(), order);
        Ok(order_id)
    }

    pub async fn cancel_order(&self, order_id: &str) -> Result<bool, Status> {
        Ok(self.pending_orders.write().await.remove(order_id).is_some())
    }

    pub async fn get_order_status(&self, order_id: &str) -> Result<Option<OrderResponse>, Status> {
        Ok(self.order_responses.read().await.get(order_id).cloned())
    }
}

#[derive(Clone)]
pub struct BridgeServiceImpl {
    order_router: Arc<OrderRouter>,
    metrics: Arc<Metrics>,
    tx: Sender<GatewayMessage>,
}

impl BridgeServiceImpl {
    pub fn new(order_router: Arc<OrderRouter>, metrics: Arc<Metrics>, tx: Sender<GatewayMessage>) -> Self {
        Self { order_router, metrics, tx }
    }
}

#[tonic::async_trait]
impl BridgeService for BridgeServiceImpl {
    async fn health_check(&self, _request: Request<HealthCheckRequest>) -> Result<Response<HealthCheckResponse>, Status> {
        self.metrics.requests_total.inc();
        let _timer = self.metrics.requests_duration.start_timer();

        Ok(Response::new(HealthCheckResponse {
            status: "SERVING".to_string(),
            version: "4.0.0".to_string(),
            uptime_seconds: 0,
        }))
    }

    async fn submit_order(&self, request: Request<OrderRequest>) -> Result<Response<OrderResponse>, Status> {
        self.metrics.requests_total.inc();
        self.metrics.orders_received.inc();
        let _timer = self.metrics.requests_duration.start_timer();

        let req = request.into_inner();
        let order_id = self.order_router.submit_order(req.clone()).await?;

        let response = OrderResponse {
            order_id: order_id.clone(),
            client_order_id: req.client_order_id.clone(),
            venue_order_id: String::new(),
            status: "SUBMITTED".to_string(),
            filled_quantity: 0.0,
            remaining_quantity: req.quantity,
            avg_fill_price: 0.0,
            filled_at: 0,
            error_message: String::new(),
        };

        self.order_router
            .order_responses
            .write()
            .await
            .insert(order_id, response.clone());
        self.metrics.orders_processed.inc();

        Ok(Response::new(response))
    }

    async fn cancel_order(&self, request: Request<OrderRequest>) -> Result<Response<OrderResponse>, Status> {
        self.metrics.requests_total.inc();
        let _timer = self.metrics.requests_duration.start_timer();

        let req = request.into_inner();
        let _ = self.order_router.cancel_order(&req.order_id).await?;

        Ok(Response::new(OrderResponse {
            order_id: req.order_id,
            client_order_id: req.client_order_id,
            venue_order_id: String::new(),
            status: "CANCELLED".to_string(),
            filled_quantity: 0.0,
            remaining_quantity: req.quantity,
            avg_fill_price: 0.0,
            filled_at: 0,
            error_message: String::new(),
        }))
    }

    async fn get_order_status(&self, request: Request<OrderRequest>) -> Result<Response<OrderResponse>, Status> {
        self.metrics.requests_total.inc();
        let _timer = self.metrics.requests_duration.start_timer();

        let req = request.into_inner();
        if let Some(resp) = self.order_router.get_order_status(&req.order_id).await? {
            Ok(Response::new(resp))
        } else {
            Ok(Response::new(OrderResponse {
                order_id: req.order_id,
                client_order_id: req.client_order_id,
                venue_order_id: String::new(),
                status: "UNKNOWN".to_string(),
                filled_quantity: 0.0,
                remaining_quantity: req.quantity,
                avg_fill_price: 0.0,
                filled_at: 0,
                error_message: "order_not_found".to_string(),
            }))
        }
    }

    async fn get_market_data(&self, request: Request<MarketDataRequest>) -> Result<Response<MarketDataResponse>, Status> {
        self.metrics.requests_total.inc();
        let _timer = self.metrics.requests_duration.start_timer();

        let req = request.into_inner();
        let symbol = req
            .symbols
            .first()
            .cloned()
            .unwrap_or_else(|| "BTC/USDT".to_string());

        Ok(Response::new(MarketDataResponse {
            symbol,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            bid_price: 0.0,
            ask_price: 0.0,
            bid_size: 0.0,
            ask_size: 0.0,
            last_price: 0.0,
            volume_24h: 0.0,
        }))
    }

    async fn subscribe(&self, request: Request<SubscribeRequest>) -> Result<Response<SubscribeResponse>, Status> {
        self.metrics.requests_total.inc();
        let _timer = self.metrics.requests_duration.start_timer();

        let req = request.into_inner();
        self.metrics.market_data_received.inc();

        let msg = GatewayMessage {
            timestamp_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64,
            message_type: "subscribe".to_string(),
            payload: req.symbol.clone().into_bytes(),
            correlation_id: format!(
                "sub-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ),
        };
        let _ = self.tx.send(msg);

        Ok(Response::new(SubscribeResponse {
            success: true,
            subscription_id: format!("sub-{}", req.symbol),
            message: "subscription accepted".to_string(),
        }))
    }

    async fn set_config(&self, request: Request<SetConfigRequest>) -> Result<Response<SetConfigResponse>, Status> {
        self.metrics.requests_total.inc();
        let _timer = self.metrics.requests_duration.start_timer();

        let _cfg: Option<BridgeConfig> = request.into_inner().config;

        Ok(Response::new(SetConfigResponse {
            success: true,
            message: "config applied".to_string(),
        }))
    }

    async fn get_metrics(&self, _request: Request<MetricsRequest>) -> Result<Response<MetricsResponse>, Status> {
        self.metrics.requests_total.inc();
        let _timer = self.metrics.requests_duration.start_timer();

        Ok(Response::new(MetricsResponse {
            metrics_text: self.metrics.export(),
        }))
    }

    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        self.metrics.requests_total.inc();
        let _timer = self.metrics.requests_duration.start_timer();

        Ok(Response::new(PingResponse {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            status: "OK".to_string(),
        }))
    }
}

pub struct TradingGateway {
    metrics: Arc<Metrics>,
    order_router: Arc<OrderRouter>,
    message_tx: Sender<GatewayMessage>,
    message_rx: Receiver<GatewayMessage>,
}

impl TradingGateway {
    pub fn new() -> Self {
        let (tx, rx) = bounded(10000);
        Self {
            metrics: Arc::new(Metrics::new()),
            order_router: Arc::new(OrderRouter::new()),
            message_tx: tx,
            message_rx: rx,
        }
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    pub async fn serve_grpc(&self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        let bridge_service = BridgeServiceImpl::new(
            self.order_router.clone(),
            self.metrics.clone(),
            self.message_tx.clone(),
        );

        info!("gRPC server listening on {}", addr);
        Server::builder()
            .add_service(BridgeServiceServer::new(bridge_service))
            .serve(addr)
            .await?;

        Ok(())
    }

    pub fn run_message_processor(&self) -> impl std::future::Future<Output = ()> + Send {
        let rx = self.message_rx.clone();
        let metrics = self.metrics.clone();

        async move {
            info!("Message processor started");
            for msg in rx {
                debug!("Processing message: {} ({})", msg.message_type, msg.correlation_id);
                metrics.market_data_received.inc();
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }
}

impl Default for TradingGateway {
    fn default() -> Self {
        Self::new()
    }
}

#[pymodule]
fn gateway(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    #[pyclass]
    struct PyGateway {
        gateway: Arc<RwLock<TradingGateway>>,
    }

    #[pymethods]
    impl PyGateway {
        #[new]
        fn new() -> Self {
            Self {
                gateway: Arc::new(RwLock::new(TradingGateway::new())),
            }
        }

        fn get_metrics(&self) -> String {
            let gateway = self.gateway.blocking_read();
            gateway.metrics().export()
        }

        fn start_grpc_server_sync(&self, host: String, port: u16) -> PyResult<String> {
            let gateway_ref = self.gateway.clone();
            let addr_str = format!("{}:{}", host, port);
            let addr: SocketAddr = addr_str.parse::<SocketAddr>().map_err(|e: std::net::AddrParseError| {
                pyo3::exceptions::PyValueError::new_err(e.to_string())
            })?;

            std::thread::spawn(move || {
                match tokio::runtime::Runtime::new() {
                    Ok(rt) => {
                        rt.block_on(async move {
                            let gateway = gateway_ref.read().await;
                            if let Err(e) = gateway.serve_grpc(addr).await {
                                error!("gRPC server error: {}", e);
                            }
                        });
                    }
                    Err(e) => error!("Failed to create tokio runtime: {}", e),
                }
            });

            Ok(format!("gRPC server starting on {}:{}", host, port))
        }
    }

    m.add_class::<PyGateway>()?;
    Ok(())
}
