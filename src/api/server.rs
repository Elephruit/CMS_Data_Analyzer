use axum::{
    routing::get,
    Router,
    Json,
    extract::State,
};
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use serde_json::{json, Value};
use std::path::Path;
use std::sync::Arc;
use crate::query::read_api::QueryEngine;

pub async fn start_server(port: u16, store_dir: &Path) -> anyhow::Result<()> {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    log::info!("Starting API server on http://{}", addr);

    let engine = Arc::new(QueryEngine::new(store_dir));

    let app = Router::new()
        .route("/api/status", get(get_status))
        .route("/api/query/filter-options", axum::routing::post(get_filter_options))
        .route("/api/query/dashboard-summary", axum::routing::post(get_dashboard_summary))
        .route("/api/query/global-trend", axum::routing::post(get_global_trend))
        .route("/api/query/top-movers", axum::routing::post(get_top_movers))
        .route("/api/query/explorer", axum::routing::post(get_explorer_data))
        .route("/api/query/organization-analysis", axum::routing::post(get_org_analysis))
        .route("/api/query/geo-analysis", axum::routing::post(get_geo_analysis))
        .route("/api/query/growth-analytics", axum::routing::post(get_growth_analytics))
        .route("/api/query/plan-details", axum::routing::post(get_plan_details))

        .route("/api/data/months", get(get_ingested_months))
        .route("/api/data/ingest", axum::routing::post(trigger_ingest))
        // Serve frontend static files
        .fallback_service(ServeDir::new("frontend/dist"))
        .layer(CorsLayer::permissive())
        .with_state(engine);

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

async fn get_filter_options(State(engine): State<Arc<QueryEngine>>, Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let start = std::time::Instant::now();
    let res = match engine.get_filter_options(&payload) {
        Ok(options) => Ok(Json(options)),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };
    log::info!("get_filter_options took {:?}", start.elapsed());
    res
}

async fn get_dashboard_summary(State(engine): State<Arc<QueryEngine>>, Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let start = std::time::Instant::now();
    let res = match engine.get_dashboard_summary(&payload) {
        Ok(summary) => Ok(Json(summary)),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };
    log::info!("get_dashboard_summary took {:?}", start.elapsed());
    res
}

async fn get_global_trend(State(engine): State<Arc<QueryEngine>>, Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let start = std::time::Instant::now();
    let res = match engine.get_global_trend(&payload) {
        Ok(trend) => Ok(Json(json!(trend))),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };
    log::info!("get_global_trend took {:?}", start.elapsed());
    res
}

async fn get_top_movers(State(engine): State<Arc<QueryEngine>>, Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let start = std::time::Instant::now();
    let state = payload["state"].as_str().map(|s| s.to_string());
    let from_str = payload["from"].as_str().unwrap_or("2025-01");
    let to_str = payload["to"].as_str().unwrap_or("2025-02");
    
    let from: crate::model::YearMonth = from_str.parse().map_err(|_| (axum::http::StatusCode::BAD_REQUEST, "Invalid from month".to_string()))?;
    let to: crate::model::YearMonth = to_str.parse().map_err(|_| (axum::http::StatusCode::BAD_REQUEST, "Invalid to month".to_string()))?;
    let limit = payload["limit"].as_u64().unwrap_or(10) as usize;

    let res = match engine.get_top_movers(state, from, to, limit) {
        Ok(movers) => Ok(Json(json!(movers))),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };
    log::info!("get_top_movers took {:?}", start.elapsed());
    res
}

async fn get_explorer_data(State(engine): State<Arc<QueryEngine>>, Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let start = std::time::Instant::now();
    let res = match engine.get_explorer_data(&payload) {
        Ok(data) => Ok(Json(data)),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };
    log::info!("get_explorer_data took {:?}", start.elapsed());
    res
}

async fn get_org_analysis(State(engine): State<Arc<QueryEngine>>, Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let start = std::time::Instant::now();
    let res = match engine.get_org_analysis(&payload) {
        Ok(data) => Ok(Json(data)),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };
    log::info!("get_org_analysis took {:?}", start.elapsed());
    res
}

async fn get_geo_analysis(State(engine): State<Arc<QueryEngine>>, Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let start = std::time::Instant::now();
    let res = match engine.get_geo_analysis(&payload) {
        Ok(data) => Ok(Json(data)),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };
    log::info!("get_geo_analysis took {:?}", start.elapsed());
    res
}

async fn get_growth_analytics(State(engine): State<Arc<QueryEngine>>, Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let start = std::time::Instant::now();
    let res = match engine.get_growth_analytics(&payload) {
        Ok(data) => Ok(Json(data)),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };
    log::info!("get_growth_analytics took {:?}", start.elapsed());
    res
}

async fn get_plan_details(State(engine): State<Arc<QueryEngine>>, Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let start = std::time::Instant::now();
    let contract_id = payload["contract_id"].as_str().ok_or((axum::http::StatusCode::BAD_REQUEST, "Missing contract_id".to_string()))?;
    let plan_id = payload["plan_id"].as_str().ok_or((axum::http::StatusCode::BAD_REQUEST, "Missing plan_id".to_string()))?;

    let res = match engine.get_plan_details(contract_id, plan_id) {
        Ok(data) => Ok(Json(data)),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };
    log::info!("get_plan_details took {:?}", start.elapsed());
    res
}

async fn get_ingested_months() -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let store_dir = Path::new("store");
    let manifest_path = store_dir.join("manifests").join("months.json");
    match crate::storage::manifests::load_manifest(&manifest_path) {
        Ok(manifest) => Ok(Json(json!(manifest.ingested_months))),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn trigger_ingest(Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let store_dir = Path::new("store");
    let month_str = payload["month"].as_str().ok_or((axum::http::StatusCode::BAD_REQUEST, "Missing month".to_string()))?;
    let month: crate::model::YearMonth = month_str.parse().map_err(|_| (axum::http::StatusCode::BAD_REQUEST, "Invalid month format".to_string()))?;
    let force = payload["force"].as_bool().unwrap_or(false);

    match crate::ingest::ingest_month(month, force, store_dir).await {
        Ok(_) => {
            log::info!("Ingested {} successfully. Cache rebuild recommended.", month_str);
            Ok(Json(json!({ "status": "success", "month": month_str })))
        },
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}
