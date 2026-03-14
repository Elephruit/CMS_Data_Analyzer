use axum::{
    routing::get,
    Router,
    Json,
};
use std::net::SocketAddr;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use serde_json::{json, Value};
use std::path::Path;

pub async fn start_server(port: u16, store_dir: &Path) -> anyhow::Result<()> {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    log::info!("Starting API server on http://{}", addr);

    let app = Router::new()
        .route("/api/status", get(get_status))
        // Serve frontend static files
        .fallback_service(ServeDir::new("frontend/dist"))
        .layer(CorsLayer::permissive());

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn get_status() -> Json<Value> {
    Json(json!({
        "status": "online",
        "version": "0.1.0-alpha",
        "message": "CMS Enrollment Analytics API is active"
    }))
}
