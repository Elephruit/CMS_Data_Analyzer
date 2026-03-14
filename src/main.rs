mod cli;
mod model;
mod cms;
mod ingest;
mod storage;
mod query;
mod util;
mod api;

use clap::Parser;
use cli::{Cli, Commands};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logger
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();
    let store_dir = std::path::Path::new("store");

    match cli.command {
        Commands::FetchMonth { month, force } => {
            log::info!("Fetching month: {}, force: {}", month, force);
            let month: model::YearMonth = month.parse()?;
            ingest::ingest_month(month, force, store_dir).await?;
        }
        Commands::FetchRange { from, to, force } => {
            log::info!("Fetching range: from {} to {}, force: {}", from, to, force);
            let start_month: model::YearMonth = from.parse()?;
            let end_month: model::YearMonth = to.parse()?;

            let mut current = start_month;
            while current <= end_month {
                log::info!("Processing month in range: {}", current);
                if let Err(e) = ingest::ingest_month(current, force, store_dir).await {
                    log::error!("Failed to ingest month {}: {}", current, e);
                }

                // Increment month
                let (next_year, next_month) = if current.month == 12 {
                    (current.year + 1, 1)
                } else {
                    (current.year, current.month + 1)
                };
                current = model::YearMonth::new(next_year, next_month)?;
            }
        }
        Commands::ListMonths => {
            log::info!("Listing months");
            let manifest_path = store_dir.join("manifests").join("months.json");
            let manifest = storage::manifests::load_manifest(&manifest_path)?;
            println!("Ingested months:");
            for month in manifest.ingested_months {
                println!("- {}", month);
            }
        }
        Commands::ValidateStore => {
            log::info!("Validating store");
            
            let plan_dim_path = store_dir.join("dims").join("plan_dim.parquet");
            let plans = storage::parquet_store::load_plan_dim(&plan_dim_path)?;
            println!("Plan Dimension: {} records", plans.len());

            let county_dim_path = store_dir.join("dims").join("county_dim.parquet");
            let counties = storage::parquet_store::load_county_dim(&county_dim_path)?;
            println!("County Dimension: {} records", counties.len());

            let manifest_path = store_dir.join("manifests").join("months.json");
            let manifest = storage::manifests::load_manifest(&manifest_path)?;
            println!("Manifest: {} months ingested", manifest.ingested_months.len());

            for month in manifest.ingested_months {
                let year_dir = store_dir.join("facts").join(format!("year={}", month.year));
                if year_dir.exists() {
                    let state_count = std::fs::read_dir(year_dir)?.filter(|e| e.as_ref().unwrap().path().is_dir()).count();
                    println!("- {}: Found {} state partitions", month, state_count);
                } else {
                    println!("- {}: Year directory MISSING at {}", month, year_dir.display());
                }
            }
        }
        Commands::RebuildCache => {
            log::info!("Rebuilding cache");
            let cache_dir = store_dir.join("cache");
            std::fs::create_dir_all(&cache_dir)?;

            // 1. Plan Lookup
            let plan_dim_path = store_dir.join("dims").join("plan_dim.parquet");
            let plans = storage::parquet_store::load_plan_dim(&plan_dim_path)?;
            let plan_map: std::collections::HashMap<u32, model::PlanDim> = plans.into_iter().map(|p| (p.plan_key, p)).collect();
            storage::binary_cache::save_plan_lookup(&plan_map, &cache_dir.join("plan_lookup.bin"))?;
            log::info!("Cached {} plans", plan_map.len());

            // 2. County Lookup
            let county_dim_path = store_dir.join("dims").join("county_dim.parquet");
            let counties = storage::parquet_store::load_county_dim(&county_dim_path)?;
            // Use natural key for the primary lookup file, but QueryEngine will optimize it
            let county_map: std::collections::HashMap<String, model::CountyDim> = counties.into_iter().map(|c| (format!("{}|{}", c.state_code, c.county_name), c)).collect();
            storage::binary_cache::save_county_lookup(&county_map, &cache_dir.join("county_lookup.bin"))?;
            log::info!("Cached {} counties", county_map.len());

            // 3. Series Cache
            // Series are partitioned by year, so the same (plan_key, county_key) may appear
            // in multiple year partitions (e.g. year=2024 has Dec data, year=2025 has Jan/Feb).
            // We must MERGE them rather than overwrite, or earlier months get dropped.
            let facts_dir = store_dir.join("facts");
            let mut all_series: std::collections::HashMap<(u32, u32), model::PlanCountySeries> = std::collections::HashMap::new();
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
                        let series_list = storage::parquet_store::load_series_partition(&series_path)?;
                        for new_s in series_list {
                            let key = (new_s.plan_key, new_s.county_key);
                            if let Some(existing) = all_series.get_mut(&key) {
                                // Merge: decode each month from new_s and add into existing
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
            storage::binary_cache::save_series_cache(&all_series, &cache_dir.join("series_values.bin"))?;
            log::info!("Cached {} series", all_series.len());
        }
        Commands::Serve { port } => {
            log::info!("Starting server on port {}", port);
            api::server::start_server(port, store_dir).await?;
        }
        Commands::ListPlans { limit } => {
            let plan_dim_path = store_dir.join("dims").join("plan_dim.parquet");
            let plans = storage::parquet_store::load_plan_dim(&plan_dim_path)?;
            println!("Listing first {} plans:", limit);
            for plan in plans.iter().take(limit) {
                println!("{}|{}: {} (Org: {}, Type: {}, Key: {})", plan.contract_id, plan.plan_id, plan.plan_name, plan.parent_org, plan.plan_type, plan.plan_key);
            }
        }
        Commands::Query { export, query_command } => {
            let engine = query::read_api::QueryEngine::new(store_dir);
            let mut results_json = serde_json::Value::Null;

            match query_command {
                cli::QueryCommands::PlanTrend { contract, plan, state, county } => {
                    log::info!("Querying plan trend: contract: {}, plan: {}, state: {:?}, county: {:?}", contract, plan, state, county);
                    
                    if let Some(plan_key) = engine.get_plan_key(&contract, &plan)? {
                        let trend = engine.get_plan_trend(plan_key)?;
                        println!("Trend for {}|{}:", contract, plan);
                        for (month, enrollment) in &trend {
                            println!("{}: {}", month, enrollment);
                        }
                        results_json = serde_json::json!({
                            "contract_id": contract,
                            "plan_id": plan,
                            "trend": trend
                        });
                    } else {
                        println!("Plan not found: {}|{}", contract, plan);
                    }
                }
                cli::QueryCommands::CountySnapshot { state, county, month } => {
                    log::info!("Querying county snapshot: state: {}, county: {}, month: {}", state, county, month);
                    let ym: model::YearMonth = month.parse()?;
                    
                    if let Some(county_key) = engine.get_county_key(&state, &county)? {
                        let snapshot = engine.get_county_snapshot(county_key, ym)?;
                        println!("Snapshot for {}, {} in {}:", county, state, ym);
                        println!("{:<10} {:<10} {:<40} {:<10}", "Contract", "Plan", "Name", "Enrollment");
                        println!("{:-<10} {:-<10} {:-<40} {:-<10}", "", "", "", "");
                        for (cid, pid, name, enrollment) in &snapshot {
                            println!("{:<10} {:<10} {:<40} {:<10}", cid, pid, name, enrollment);
                        }
                        results_json = serde_json::json!({
                            "state": state,
                            "county": county,
                            "month": month,
                            "snapshot": snapshot
                        });
                    } else {
                        println!("County not found: {}, {}", county, state);
                    }
                }
                cli::QueryCommands::StateRollup { state, from, to } => {
                    log::info!("Querying state rollup: state: {}, from: {}, to: {}", state, from, to);
                    let start_month: model::YearMonth = from.parse()?;
                    let end_month: model::YearMonth = to.parse()?;
                    
                    let rollup = engine.get_state_rollup(&state, start_month, end_month)?;
                    println!("Rollup for {}:", state.to_uppercase());
                    for (month, enrollment) in &rollup {
                        println!("{}: {}", month, enrollment);
                    }
                    results_json = serde_json::json!({
                        "state": state,
                        "from": from,
                        "to": to,
                        "rollup": rollup
                    });
                }
                cli::QueryCommands::TopMovers { state, from, to, limit } => {
                    log::info!("Querying top movers: state: {:?}, from: {}, to: {}, limit: {}", state, from, to, limit);
                    let start_month: model::YearMonth = from.parse()?;
                    let end_month: model::YearMonth = to.parse()?;
                    
                    let movers = engine.get_top_movers(state.clone(), start_month, end_month, limit)?;
                    println!("Top {} movers from {} to {} {}:", limit, from, to, state.clone().unwrap_or_else(|| "Nationwide".to_string()));
                    println!("{:<10} {:<10} {:<40} {:<10}", "Contract", "Plan", "Name", "Change");
                    println!("{:-<10} {:-<10} {:-<40} {:-<10}", "", "", "", "");
                    for (cid, pid, name, change, _prior) in &movers {
                        println!("{:<10} {:<10} {:<40} {:<10}", cid, pid, name, change);
                    }
                    results_json = serde_json::json!({
                        "state": state,
                        "from": from,
                        "to": to,
                        "movers": movers
                    });
                }
            }

            if let Some(path) = export {
                if !results_json.is_null() {
                    let file = std::fs::File::create(&path)?;
                    serde_json::to_writer_pretty(file, &results_json)?;
                    log::info!("Exported results to {}", path);
                }
            }
        }
    }

    Ok(())
}
