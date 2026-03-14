use axum::{
    routing::get,
    Router,
    Json,
};
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use serde_json::{json, Value};
use std::path::Path;

pub async fn start_server(port: u16, _store_dir: &Path) -> anyhow::Result<()> {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    log::info!("Starting API server on http://{}", addr);

    let app = Router::new()
        .route("/api/status", get(get_status))
        .route("/api/query/filter-options", axum::routing::post(get_filter_options))
        .route("/api/query/dashboard-summary", axum::routing::post(get_dashboard_summary))
        .route("/api/query/global-trend", axum::routing::post(get_global_trend))
        .route("/api/query/top-movers", axum::routing::post(get_top_movers))
        .route("/api/query/explorer", axum::routing::post(get_explorer_data))
        .route("/api/query/organization-analysis", axum::routing::post(get_org_analysis))

        .route("/api/data/months", get(get_ingested_months))
        .route("/api/data/ingest", axum::routing::post(trigger_ingest))
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

async fn get_filter_options(Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let store_dir = Path::new("store");
    let engine = crate::query::read_api::QueryEngine::new(store_dir);
    
    match engine.get_filter_options(&payload) {
        Ok(options) => Ok(Json(options)),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn get_dashboard_summary(Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let store_dir = Path::new("store");
    let engine = crate::query::read_api::QueryEngine::new(store_dir);
    
    match engine.get_dashboard_summary(&payload) {
        Ok(summary) => Ok(Json(summary)),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn get_global_trend(Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let store_dir = Path::new("store");
    let engine = crate::query::read_api::QueryEngine::new(store_dir);
    
    match engine.get_global_trend(&payload) {
        Ok(trend) => Ok(Json(json!(trend))),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn get_top_movers(Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let store_dir = Path::new("store");
    let engine = crate::query::read_api::QueryEngine::new(store_dir);
    
    let state = payload["state"].as_str().map(|s| s.to_string());
    // Parse months from payload or use defaults
    let from_str = payload["from"].as_str().unwrap_or("2025-01");
    let to_str = payload["to"].as_str().unwrap_or("2025-02");
    
    let from: crate::model::YearMonth = from_str.parse().map_err(|_| (axum::http::StatusCode::BAD_REQUEST, "Invalid from month".to_string()))?;
    let to: crate::model::YearMonth = to_str.parse().map_err(|_| (axum::http::StatusCode::BAD_REQUEST, "Invalid to month".to_string()))?;
    let limit = payload["limit"].as_u64().unwrap_or(10) as usize;

    match engine.get_top_movers(state, from, to, limit) {
        Ok(movers) => Ok(Json(json!(movers))),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn get_explorer_data(Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let store_dir = Path::new("store");
    let engine = crate::query::read_api::QueryEngine::new(store_dir);
    
    match engine.get_explorer_data(&payload) {
        Ok(data) => Ok(Json(data)),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn get_org_analysis(Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let store_dir = Path::new("store");
    let engine = crate::query::read_api::QueryEngine::new(store_dir);
    
    match engine.get_org_analysis(&payload) {
        Ok(data) => Ok(Json(data)),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn get_ingested_months() -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let store_dir = Path::new("store");
    let manifest_path = store_dir.join("manifests").join("months.json");
    match crate::storage::manifests::load_manifest(&manifest_path) {
        Ok(manifest) => Ok(Json(json!(manifest.ingested_months))),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

#[axum::debug_handler]
async fn trigger_ingest(Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let store_dir = Path::new("store");
    let month_str = payload["month"].as_str().ok_or((axum::http::StatusCode::BAD_REQUEST, "Missing month".to_string()))?;
    let month: crate::model::YearMonth = month_str.parse().map_err(|_| (axum::http::StatusCode::BAD_REQUEST, "Invalid month format".to_string()))?;
    let force = payload["force"].as_bool().unwrap_or(false);

    match crate::ingest::ingest_month(month, force, store_dir).await {
        Ok(_) => {
            // In a real app, we'd trigger cache rebuild here
            log::info!("Ingested {} successfully. Cache rebuild recommended.", month_str);
            Ok(Json(json!({ "status": "success", "month": month_str })))
        },
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}
