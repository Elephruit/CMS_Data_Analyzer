use axum::{
    routing::get,
    Router,
    Json,
    extract::State,
    response::IntoResponse,
};
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use serde_json::{json, Value};
use std::path::Path;
use std::sync::Arc;
use tokio::net::TcpListener;
use crate::query::read_api::QueryEngine;

pub async fn start_server(engine: Arc<QueryEngine>, port: u16) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/api/dashboard/summary", axum::routing::post(get_dashboard_summary))
        .route("/api/dashboard/trend", axum::routing::post(get_global_trend))
        .route("/api/explorer", axum::routing::post(get_explorer_data))
        .route("/api/organizations", axum::routing::post(get_org_analysis))
        .route("/api/plans", axum::routing::post(get_plan_list))
        .route("/api/geography", axum::routing::post(get_geo_analysis))
        .route("/api/filter-options", axum::routing::post(get_filter_options))
        .route("/api/growth", axum::routing::post(get_growth_analytics))
        .route("/api/crosswalk/analysis", axum::routing::post(get_crosswalk_analysis))
        .route("/api/crosswalk/aep-switching", axum::routing::post(get_aep_switching))
        .route("/api/crosswalk/lineage", get(get_plan_lineage))
        .route("/api/plans/details", get(get_plan_details))
        
        // Data Management Routes
        .route("/api/data/months", get(get_ingested_months))
        .route("/api/data/ingest", axum::routing::post(trigger_ingest))
        .route("/api/data/delete-month", axum::routing::post(delete_month))
        .route("/api/data/delete-year", axum::routing::post(delete_year))

        .route("/api/data/landscape/status", get(get_landscape_status))
        .route("/api/data/landscape/discover", get(trigger_landscape_discovery))
        .route("/api/data/landscape/ingest", axum::routing::post(trigger_landscape_ingest))

        .route("/api/data/crosswalk/status", get(get_crosswalk_status))
        .route("/api/data/crosswalk/discover", get(trigger_crosswalk_discovery))
        .route("/api/data/crosswalk/ingest", axum::routing::post(trigger_crosswalk_ingest))
        
        // Serve frontend static files
        .fallback_service(ServeDir::new("frontend/dist"))
        .layer(CorsLayer::permissive())
        .with_state(engine);

    let addr = format!("0.0.0.0:{}", port);
    log::info!("Server starting on {}", addr);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn get_dashboard_summary(State(engine): State<Arc<QueryEngine>>, Json(filters): Json<Value>) -> impl IntoResponse {
    match engine.get_dashboard_summary(&filters) {
        Ok(data) => Json(data).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_global_trend(State(engine): State<Arc<QueryEngine>>, Json(filters): Json<Value>) -> impl IntoResponse {
    match engine.get_global_trend(&filters) {
        Ok(data) => Json(data).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_explorer_data(State(engine): State<Arc<QueryEngine>>, Json(payload): Json<Value>) -> impl IntoResponse {
    match engine.get_explorer_data(&payload) {
        Ok(data) => Json(data).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_org_analysis(State(engine): State<Arc<QueryEngine>>, Json(filters): Json<Value>) -> impl IntoResponse {
    match engine.get_org_analysis(&filters) {
        Ok(data) => Json(data).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_plan_list(State(engine): State<Arc<QueryEngine>>, Json(filters): Json<Value>) -> impl IntoResponse {
    match engine.get_plan_list(&filters) {
        Ok(data) => Json(data).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_geo_analysis(State(engine): State<Arc<QueryEngine>>, Json(filters): Json<Value>) -> impl IntoResponse {
    match engine.get_geo_analysis(&filters) {
        Ok(data) => Json(data).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_filter_options(State(engine): State<Arc<QueryEngine>>, Json(filters): Json<Value>) -> impl IntoResponse {
    match engine.get_filter_options(&filters) {
        Ok(data) => Json(data).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_growth_analytics(State(engine): State<Arc<QueryEngine>>, Json(filters): Json<Value>) -> impl IntoResponse {
    match engine.get_growth_analytics(&filters) {
        Ok(data) => Json(data).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_crosswalk_analysis(State(engine): State<Arc<QueryEngine>>, Json(filters): Json<Value>) -> impl IntoResponse {
    match engine.get_crosswalk_analysis(&filters) {
        Ok(data) => Json(data).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_aep_switching(State(engine): State<Arc<QueryEngine>>, Json(filters): Json<Value>) -> impl IntoResponse {
    match engine.get_aep_switching(&filters) {
        Ok(data) => Json(data).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_plan_lineage(
    State(engine): State<Arc<QueryEngine>>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>
) -> impl IntoResponse {
    let contract_id = params.get("contract_id").map(|s| s.as_str()).unwrap_or("");
    let plan_id = params.get("plan_id").map(|s| s.as_str()).unwrap_or("");
    let year = params.get("year").and_then(|s| s.parse::<i32>().ok()).unwrap_or(2025);
    match engine.lineage_engine.get_lineage(contract_id, plan_id, year) {
        Ok(data) => Json(data).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_plan_details(
    State(engine): State<Arc<QueryEngine>>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>
) -> impl IntoResponse {
    let contract_id = params.get("contract_id").map(|s| s.as_str()).unwrap_or("");
    let plan_id = params.get("plan_id").map(|s| s.as_str()).unwrap_or("");
    match engine.get_plan_details(contract_id, plan_id) {
        Ok(data) => Json(data).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

use std::collections::HashMap;

// Data Management Handlers
async fn get_ingested_months() -> impl IntoResponse {
    let store_dir = Path::new("store");
    let manifest_path = store_dir.join("manifests").join("months.json");
    match crate::storage::manifests::load_manifest(&manifest_path) {
        Ok(m) => Json(m.ingested_months).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn trigger_ingest(Json(payload): Json<Value>) -> impl IntoResponse {
    let month_str = payload["month"].as_str().unwrap_or("");
    if let Ok(month) = month_str.parse::<crate::model::YearMonth>() {
        let store_dir = Path::new("store");
        match crate::ingest::ingest_month(month, false, store_dir).await {
            Ok(_) => Json(json!({ "status": "success" })).into_response(),
            Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        }
    } else {
        (axum::http::StatusCode::BAD_REQUEST, "Invalid month format").into_response()
    }
}

async fn delete_month(Json(payload): Json<Value>) -> impl IntoResponse {
    let month_str = payload["month"].as_str().unwrap_or("");
    if let Ok(month) = month_str.parse::<crate::model::YearMonth>() {
        let store_dir = Path::new("store");
        match crate::ingest::delete_month(month, store_dir) {
            Ok(_) => Json(json!({ "status": "success" })).into_response(),
            Err(e) => {
                let err_msg: String = e.to_string();
                (axum::http::StatusCode::INTERNAL_SERVER_ERROR, err_msg).into_response()
            }
        }
    } else {
        (axum::http::StatusCode::BAD_REQUEST, "Invalid month format").into_response()
    }
}

async fn delete_year(Json(payload): Json<Value>) -> impl IntoResponse {
    let year = payload["year"].as_i64().unwrap_or(0) as i32;
    let store_dir = Path::new("store");
    
    // Find all months for this year in manifest
    let manifest_path = store_dir.join("manifests").join("months.json");
    let manifest = match crate::storage::manifests::load_manifest(&manifest_path) {
        Ok(m) => m,
        Err(e) => return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    for month in manifest.ingested_months {
        if month.year == year {
            if let Err(e) = crate::ingest::delete_month(month, store_dir) {
                log::error!("Failed to delete month {}: {}", month, e);
            }
        }
    }

    // Rebuild cache after bulk delete
    match crate::storage::binary_cache::rebuild_cache(store_dir) {
        Ok(_) => {
            log::info!("Cache rebuilt after delete of year {}", year);
        }
        Err(e) => {
            log::error!("Cache rebuild failed after delete of year {}: {}", year, e);
        }
    }

    Json(json!({ "status": "deleted", "year": year })).into_response()
}

async fn get_landscape_status() -> impl IntoResponse {
    let store_dir = Path::new("store");
    let manifest_path = store_dir.join("landscape").join("manifests").join("landscape_manifest.json");
    
    if !manifest_path.exists() {
        return Json(json!({
            "status": "not_discovered",
            "imported_years": [],
            "available_years": []
        })).into_response();
    }

    match std::fs::File::open(&manifest_path) {
        Ok(file) => {
            match serde_json::from_reader::<_, crate::model::landscape::LandscapeManifest>(file) {
                Ok(manifest) => {
                    let years: std::collections::BTreeSet<i32> = manifest.files.iter().map(|f| f.year).filter(|&y| y > 0).collect();
                    Json(json!({
                        "status": "active",
                        "imported_years": manifest.imported_years,
                        "available_years": years.into_iter().collect::<Vec<_>>()
                    })).into_response()
                },
                Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            }
        }
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn trigger_landscape_discovery() -> impl IntoResponse {
    log::info!("Triggering programmatic Landscape discovery from CMS...");

    let discovery = match crate::cms::discover::discover_landscape_archives().await {
        Ok(d) => d,
        Err(e) => return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    let store_dir = Path::new("store");
    let landscape_dir = store_dir.join("landscape");
    let raw_dir = landscape_dir.join("raw");
    if let Err(e) = std::fs::create_dir_all(&raw_dir) {
        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }

    let mut all_files = Vec::new();
    let mut source_archives = std::collections::HashMap::new();

    // 1. Process Standalone ZIP (e.g. CY2026)
    if let Some(standalone_url) = discovery.standalone_zip_url {
        log::info!("Fetching standalone Landscape ZIP: {}", standalone_url);
        match crate::ingest::landscape::process_archive_from_url(&standalone_url, &raw_dir).await {
            Ok((name, mut files)) => {
                let local_path = raw_dir.join(&name);
                source_archives.insert(name, local_path.to_string_lossy().to_string());
                all_files.append(&mut files);
            },
            Err(e) => log::warn!("Failed to process standalone ZIP {}: {}", standalone_url, e),
        }
    }

    // 2. Process Historical Archive (e.g. CY2006-CY2025)
    if let Some(historical_url) = discovery.historical_archive_url {
        log::info!("Fetching historical Landscape archive: {}", historical_url);
        match crate::ingest::landscape::process_archive_from_url(&historical_url, &raw_dir).await {
            Ok((name, mut files)) => {
                let local_path = raw_dir.join(&name);
                source_archives.insert(name, local_path.to_string_lossy().to_string());
                all_files.append(&mut files);
            },
            Err(e) => log::warn!("Failed to process historical archive {}: {}", historical_url, e),
        }
    }

    if all_files.is_empty() {
        return (axum::http::StatusCode::NOT_FOUND, "No Landscape files discovered").into_response();
    }

    let manifest_path = landscape_dir.join("manifests").join("landscape_manifest.json");
    if let Err(e) = std::fs::create_dir_all(manifest_path.parent().unwrap()) {
        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }
    
    // Load existing manifest to preserve imported_years
    let mut manifest = if manifest_path.exists() {
        match std::fs::File::open(&manifest_path) {
            Ok(file) => serde_json::from_reader(file).unwrap_or(crate::model::landscape::LandscapeManifest::default()),
            Err(_) => crate::model::landscape::LandscapeManifest::default(),
        }
    } else {
        crate::model::landscape::LandscapeManifest::default()
    };

    manifest.files = all_files;
    manifest.source_archives = source_archives;
    
    match std::fs::File::create(&manifest_path) {
        Ok(file) => {
            if let Err(e) = serde_json::to_writer_pretty(file, &manifest) {
                return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        },
        Err(e) => return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
    
    Json(json!({ "status": "success", "entries": manifest.files.len() })).into_response()
}

async fn trigger_landscape_ingest(Json(payload): Json<Value>) -> impl IntoResponse {
    let year = payload["year"].as_i64().ok_or((axum::http::StatusCode::BAD_REQUEST, "Missing year".to_string()));
    let year = match year {
        Ok(y) => y as i32,
        Err(e) => return e.into_response(),
    };
    let force = payload["force"].as_bool().unwrap_or(false);
    let store_dir = Path::new("store");

    match crate::ingest::landscape::ingest_landscape_year(year, force, store_dir).await {
        Ok(_) => Json(json!({ "status": "success", "year": year })).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_crosswalk_status() -> impl IntoResponse {
    let store_dir = Path::new("store");
    let manifest_path = store_dir.join("crosswalk").join("manifests").join("crosswalk_manifest.json");
    
    if !manifest_path.exists() {
        return Json(json!({
            "status": "not_discovered",
            "imported_years": [],
            "available_years": []
        })).into_response();
    }

    match std::fs::File::open(&manifest_path) {
        Ok(file) => {
            match serde_json::from_reader::<_, crate::model::CrosswalkManifest>(file) {
                Ok(manifest) => {
                    let years: std::collections::BTreeSet<i32> = manifest.files.iter().map(|f| f.year).filter(|&y| y > 0).collect();
                    Json(json!({
                        "status": "active",
                        "imported_years": manifest.imported_years,
                        "available_years": years.into_iter().collect::<Vec<_>>()
                    })).into_response()
                },
                Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            }
        }
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn trigger_crosswalk_discovery() -> impl IntoResponse {
    log::info!("Triggering programmatic Crosswalk discovery from CMS...");

    match crate::ingest::crosswalk::discover_crosswalk_archives_full().await {
        Ok(manifest) => {
            let store_dir = Path::new("store");
            let crosswalk_dir = store_dir.join("crosswalk");
            if let Err(e) = std::fs::create_dir_all(&crosswalk_dir) {
                return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
            let manifest_path = crosswalk_dir.join("manifests").join("crosswalk_manifest.json");
            if let Err(e) = std::fs::create_dir_all(manifest_path.parent().unwrap()) {
                return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
            
            match std::fs::File::create(&manifest_path) {
                Ok(file) => {
                    if let Err(e) = serde_json::to_writer_pretty(file, &manifest) {
                        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
                    }
                },
                Err(e) => return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            }
            
            Json(json!({ "status": "success", "entries": manifest.files.len() })).into_response()
        }
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn trigger_crosswalk_ingest(Json(payload): Json<Value>) -> impl IntoResponse {
    let year = payload["year"].as_i64().ok_or((axum::http::StatusCode::BAD_REQUEST, "Missing year".to_string()));
    let year = match year {
        Ok(y) => y as i32,
        Err(e) => return e.into_response(),
    };
    let force = payload["force"].as_bool().unwrap_or(false);
    let store_dir = Path::new("store");

    match crate::ingest::crosswalk::ingest_crosswalk_year(year, force, store_dir).await {
        Ok(_) => Json(json!({ "status": "success", "year": year })).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
