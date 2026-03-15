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
use tokio::sync::RwLock;
use crate::query::read_api::QueryEngine;
use crate::model::PlanCountySeries;

type EngineState = Arc<RwLock<QueryEngine>>;

pub async fn start_server(port: u16, store_dir: &Path) -> anyhow::Result<()> {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    log::info!("Starting API server on http://{}", addr);

    let engine = Arc::new(RwLock::new(QueryEngine::new(store_dir)));

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
        .route("/api/query/plan-list", axum::routing::post(get_plan_list))

        .route("/api/data/months", get(get_ingested_months))
        .route("/api/data/ingest", axum::routing::post(trigger_ingest))
        .route("/api/data/delete-month", axum::routing::post(delete_month))
        .route("/api/data/delete-year", axum::routing::post(delete_year))

        .route("/api/data/landscape/status", get(get_landscape_status))
        .route("/api/data/landscape/discover", get(trigger_landscape_discovery))
        .route("/api/data/landscape/ingest", axum::routing::post(trigger_landscape_ingest))
        
        // Serve frontend static files
        .fallback_service(ServeDir::new("frontend/dist"))
        .layer(CorsLayer::permissive())
        .with_state(engine);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

fn rebuild_and_reload(store_dir: &Path) -> anyhow::Result<QueryEngine> {
    let cache_dir = store_dir.join("cache");
    std::fs::create_dir_all(&cache_dir)?;

    // 1. Plan Lookup
    let plan_dim_path = store_dir.join("dims").join("plan_dim.parquet");
    let plans = crate::storage::parquet_store::load_plan_dim(&plan_dim_path)?;
    let plan_count = plans.len();
    let plan_map: std::collections::HashMap<u32, crate::model::PlanDim> = plans.into_iter().map(|p| (p.plan_key, p)).collect();
    crate::storage::binary_cache::save_plan_lookup(&plan_map, &cache_dir.join("plan_lookup.bin"))?;

    // 2. County Lookup
    let county_dim_path = store_dir.join("dims").join("county_dim.parquet");
    let counties = crate::storage::parquet_store::load_county_dim(&county_dim_path)?;
    let county_count = counties.len();
    let county_map: std::collections::HashMap<String, crate::model::CountyDim> = counties
        .into_iter()
        .map(|c| (format!("{}|{}", c.state_code, c.county_name), c))
        .collect();
    crate::storage::binary_cache::save_county_lookup(&county_map, &cache_dir.join("county_lookup.bin"))?;

    // 3. Series Cache — merge across year partitions
    let facts_dir = store_dir.join("facts");
    let mut all_series: std::collections::HashMap<(u32, u32), PlanCountySeries> = std::collections::HashMap::new();
    if facts_dir.exists() {
        let mut year_paths: Vec<_> = std::fs::read_dir(&facts_dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.is_dir())
            .collect();
        year_paths.sort();
        for year_path in year_paths {
            let mut state_paths: Vec<_> = std::fs::read_dir(&year_path)?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.is_dir())
                .collect();
            state_paths.sort();
            for state_path in state_paths {
                let series_path = state_path.join("plan_county_series.parquet");
                let series_list = crate::storage::parquet_store::load_series_partition(&series_path)?;
                for new_s in series_list {
                    let key = (new_s.plan_key, new_s.county_key);
                    if let Some(existing) = all_series.get_mut(&key) {
                        let bitmap = new_s.presence_bitmap;
                        let start_year = (new_s.start_month_key / 100) as i32;
                        let start_month = (new_s.start_month_key % 100) as i32;
                        let mut pos = 0usize;
                        for i in 0..64u32 {
                            if (bitmap >> i) & 1 != 0 {
                                let curr = start_month - 1 + i as i32;
                                let year = start_year + curr / 12;
                                let month = curr % 12 + 1;
                                let yyyymm = (year as u32) * 100 + month as u32;
                                if let Some(&enrollment) = new_s.enrollments.get(pos) {
                                    existing.add_month(yyyymm, enrollment);
                                }
                                pos += 1;
                            }
                        }
                    } else {
                        all_series.insert(key, new_s);
                    }
                }
            }
        }
    }
    let series_count = all_series.len();
    crate::storage::binary_cache::save_series_cache(&all_series, &cache_dir.join("series_values.bin"))?;

    log::info!("Cache rebuilt: {} plans, {} counties, {} series", plan_count, county_count, series_count);

    Ok(QueryEngine::new(store_dir))
}

async fn get_status() -> Json<Value> {
    Json(json!({
        "status": "online",
        "version": "0.1.0-alpha",
        "message": "CMS Enrollment Analytics API is active"
    }))
}

async fn get_filter_options(State(engine): State<EngineState>, Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let engine = engine.read().await;
    let start = std::time::Instant::now();
    let res = match engine.get_filter_options(&payload) {
        Ok(options) => Ok(Json(options)),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };
    log::info!("get_filter_options took {:?}", start.elapsed());
    res
}

async fn get_dashboard_summary(State(engine): State<EngineState>, Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let engine = engine.read().await;
    let start = std::time::Instant::now();
    let res = match engine.get_dashboard_summary(&payload) {
        Ok(summary) => Ok(Json(summary)),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };
    log::info!("get_dashboard_summary took {:?}", start.elapsed());
    res
}

async fn get_global_trend(State(engine): State<EngineState>, Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let engine = engine.read().await;
    let start = std::time::Instant::now();
    let res = match engine.get_global_trend(&payload) {
        Ok(trend) => Ok(Json(json!(trend))),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };
    log::info!("get_global_trend took {:?}", start.elapsed());
    res
}

async fn get_top_movers(State(engine): State<EngineState>, Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let engine = engine.read().await;
    let start = std::time::Instant::now();
    let from_str = payload["from"].as_str().unwrap_or("2025-01");
    let to_str = payload["to"].as_str().unwrap_or("2025-02");

    let from: crate::model::YearMonth = from_str.parse().map_err(|_| (axum::http::StatusCode::BAD_REQUEST, "Invalid from month".to_string()))?;
    let to: crate::model::YearMonth = to_str.parse().map_err(|_| (axum::http::StatusCode::BAD_REQUEST, "Invalid to month".to_string()))?;
    let limit = payload["limit"].as_u64().unwrap_or(10) as usize;

    let res = match engine.get_top_movers(&payload, from, to, limit) {
        Ok(movers) => Ok(Json(json!(movers))),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };
    log::info!("get_top_movers took {:?}", start.elapsed());
    res
}

async fn get_explorer_data(State(engine): State<EngineState>, Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let engine = engine.read().await;
    let start = std::time::Instant::now();
    let res = match engine.get_explorer_data(&payload) {
        Ok(data) => Ok(Json(data)),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };
    log::info!("get_explorer_data took {:?}", start.elapsed());
    res
}

async fn get_org_analysis(State(engine): State<EngineState>, Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let engine = engine.read().await;
    let start = std::time::Instant::now();
    let res = match engine.get_org_analysis(&payload) {
        Ok(data) => Ok(Json(data)),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };
    log::info!("get_org_analysis took {:?}", start.elapsed());
    res
}

async fn get_geo_analysis(State(engine): State<EngineState>, Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let engine = engine.read().await;
    let start = std::time::Instant::now();
    let res = match engine.get_geo_analysis(&payload) {
        Ok(data) => Ok(Json(data)),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };
    log::info!("get_geo_analysis took {:?}", start.elapsed());
    res
}

async fn get_growth_analytics(State(engine): State<EngineState>, Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let engine = engine.read().await;
    let start = std::time::Instant::now();
    let res = match engine.get_growth_analytics(&payload) {
        Ok(data) => Ok(Json(data)),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };
    log::info!("get_growth_analytics took {:?}", start.elapsed());
    res
}

async fn get_plan_details(State(engine): State<EngineState>, Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let engine = engine.read().await;
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

async fn get_plan_list(State(engine): State<EngineState>, Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let engine = engine.read().await;
    let start = std::time::Instant::now();
    let res = match engine.get_plan_list(&payload) {
        Ok(data) => Ok(Json(data)),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };
    log::info!("get_plan_list took {:?}", start.elapsed());
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

async fn trigger_ingest(State(engine): State<EngineState>, Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let store_dir = Path::new("store");
    let month_str = payload["month"].as_str().ok_or((axum::http::StatusCode::BAD_REQUEST, "Missing month".to_string()))?;
    let month: crate::model::YearMonth = month_str.parse().map_err(|_| (axum::http::StatusCode::BAD_REQUEST, "Invalid month format".to_string()))?;
    let force = payload["force"].as_bool().unwrap_or(false);

    match crate::ingest::ingest_month(month, force, store_dir).await {
        Ok(_) => {
            match rebuild_and_reload(store_dir) {
                Ok(new_engine) => {
                    *engine.write().await = new_engine;
                    log::info!("Engine reloaded after ingest of {}", month_str);
                }
                Err(e) => {
                    log::error!("Cache rebuild failed after ingest of {}: {}", month_str, e);
                }
            }
            Ok(Json(json!({ "status": "success", "month": month_str })))
        },
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn delete_month(State(engine): State<EngineState>, Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let store_dir = Path::new("store");
    let month_str = payload["month"].as_str().ok_or((axum::http::StatusCode::BAD_REQUEST, "Missing month".to_string()))?;
    let month: crate::model::YearMonth = month_str.parse().map_err(|_| (axum::http::StatusCode::BAD_REQUEST, "Invalid month format".to_string()))?;

    // 1. Remove from manifest
    let manifest_path = store_dir.join("manifests").join("months.json");
    let mut manifest = crate::storage::manifests::load_manifest(&manifest_path).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    manifest.ingested_months.retain(|m| *m != month);
    manifest.source_hashes.remove(&month.to_string());
    crate::storage::manifests::save_manifest(&manifest, &manifest_path).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Note: Data remains in Parquet until full year delete or vacuum.
    // Query engine should be updated to respect manifest if we want strict deletion.
    // For now, removing from manifest hides it from the UI.

    match rebuild_and_reload(store_dir) {
        Ok(new_engine) => {
            *engine.write().await = new_engine;
            log::info!("Engine reloaded after delete of month {}", month_str);
        }
        Err(e) => {
            log::error!("Cache rebuild failed after delete of month {}: {}", month_str, e);
        }
    }

    Ok(Json(json!({ "status": "deleted", "month": month_str })))
}

async fn delete_year(State(engine): State<EngineState>, Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let store_dir = Path::new("store");
    let year = payload["year"].as_i64().ok_or((axum::http::StatusCode::BAD_REQUEST, "Missing year".to_string()))? as i32;

    // 1. Remove year directory
    let year_dir = store_dir.join("facts").join(format!("year={}", year));
    if year_dir.exists() {
        std::fs::remove_dir_all(year_dir).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    // 2. Remove from manifest
    let manifest_path = store_dir.join("manifests").join("months.json");
    let mut manifest = crate::storage::manifests::load_manifest(&manifest_path).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    manifest.ingested_months.retain(|m| m.year != year);

    // Clean up hashes
    let keys_to_remove: Vec<String> = manifest.source_hashes.keys()
        .filter(|k| k.starts_with(&format!("{}-", year)))
        .cloned()
        .collect();
    for k in keys_to_remove {
        manifest.source_hashes.remove(&k);
    }

    crate::storage::manifests::save_manifest(&manifest, &manifest_path).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    match rebuild_and_reload(store_dir) {
        Ok(new_engine) => {
            *engine.write().await = new_engine;
            log::info!("Engine reloaded after delete of year {}", year);
        }
        Err(e) => {
            log::error!("Cache rebuild failed after delete of year {}: {}", year, e);
        }
    }

    Ok(Json(json!({ "status": "deleted", "year": year })))
}

async fn get_landscape_status() -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let store_dir = Path::new("store");
    let manifest_path = store_dir.join("landscape").join("manifests").join("landscape_manifest.json");
    
    if !manifest_path.exists() {
        return Ok(Json(json!({
            "status": "not_discovered",
            "imported_years": [],
            "available_years": []
        })));
    }

    match std::fs::File::open(&manifest_path) {
        Ok(file) => {
            let manifest: crate::model::landscape::LandscapeManifest = serde_json::from_reader(file).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            let years: std::collections::BTreeSet<i32> = manifest.files.iter().map(|f| f.year).filter(|&y| y > 0).collect();
            
            Ok(Json(json!({
                "status": "active",
                "imported_years": manifest.imported_years,
                "available_years": years.into_iter().collect::<Vec<_>>()
            })))
        }
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn trigger_landscape_discovery() -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    log::info!("Triggering programmatic Landscape discovery from CMS...");

    let discovery = match crate::cms::discover::discover_landscape_archives().await {
        Ok(d) => d,
        Err(e) => return Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };

    let store_dir = Path::new("store");
    let landscape_dir = store_dir.join("landscape");
    let raw_dir = landscape_dir.join("raw");
    std::fs::create_dir_all(&raw_dir).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

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
        return Err((axum::http::StatusCode::NOT_FOUND, "No Landscape files discovered".to_string()));
    }

    let manifest_path = landscape_dir.join("manifests").join("landscape_manifest.json");
    std::fs::create_dir_all(manifest_path.parent().unwrap()).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    
    // Load existing manifest to preserve imported_years
    let mut manifest = if manifest_path.exists() {
        let file = std::fs::File::open(&manifest_path).unwrap();
        serde_json::from_reader(file).unwrap_or(crate::model::landscape::LandscapeManifest::default())
    } else {
        crate::model::landscape::LandscapeManifest::default()
    };

    manifest.files = all_files;
    manifest.source_archives = source_archives;
    
    let file = std::fs::File::create(&manifest_path).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    serde_json::to_writer_pretty(file, &manifest).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    
    Ok(Json(json!({ "status": "success", "entries": manifest.files.len() })))
}

async fn trigger_landscape_ingest(Json(payload): Json<Value>) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let year = payload["year"].as_i64().ok_or((axum::http::StatusCode::BAD_REQUEST, "Missing year".to_string()))? as i32;
    let force = payload["force"].as_bool().unwrap_or(false);
    let store_dir = Path::new("store");

    match crate::ingest::landscape::ingest_landscape_year(year, force, store_dir).await {
        Ok(_) => Ok(Json(json!({ "status": "success", "year": year }))),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}
