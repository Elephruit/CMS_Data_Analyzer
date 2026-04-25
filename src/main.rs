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
        Some(Commands::FetchMonth { month, force }) => {
            log::info!("Fetching month: {}, force: {}", month, force);
            let month: model::YearMonth = month.parse()?;
            ingest::ingest_month(month, force, store_dir).await?;
        }
        Some(Commands::FetchRange { from, to, force }) => {
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
        Some(Commands::ListMonths) => {
            log::info!("Listing months");
            let manifest_path = store_dir.join("manifests").join("months.json");
            let manifest = storage::manifests::load_manifest(&manifest_path)?;
            println!("Ingested months:");
            for month in manifest.ingested_months {
                println!("- {}", month);
            }
        }
        Some(Commands::ValidateStore) => {
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
        Some(Commands::RepairDim) => {
            storage::maintenance::repair_dimension(store_dir)?;
            storage::maintenance::rebuild_cache(store_dir)?;
        }
        Some(Commands::RebuildCache) => {
            storage::maintenance::rebuild_cache(store_dir)?;
        }
        Some(Commands::Serve { port }) => {
            log::info!("Starting server on port {}", port);
            api::server::start_server(port, store_dir).await?;
        }
        Some(Commands::ListPlans { limit }) => {
            let plan_dim_path = store_dir.join("dims").join("plan_dim.parquet");
            let plans = storage::parquet_store::load_plan_dim(&plan_dim_path)?;
            println!("Listing first {} plans:", limit);
            for plan in plans.iter().take(limit) {
                println!("{}|{}: {} (Org: {}, Type: {}, Key: {})", plan.contract_id, plan.plan_id, plan.plan_name, plan.parent_org, plan.plan_type, plan.plan_key);
            }
        }
        Some(Commands::Query { export, query_command }) => {
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
                    let filters_json = if let Some(ref s) = state {
                        serde_json::json!({ "states": [s] })
                    } else {
                        serde_json::json!({})
                    };
                    let movers_json = engine.get_top_movers(&filters_json, start_month, end_month, limit)?;
                    println!("Top movers from {} to {} {}:", from, to, state.clone().unwrap_or_else(|| "Nationwide".to_string()));
                    
                    println!("\nTOP INCREASES:");
                    println!("{:<10} {:<10} {:<40} {:<10}", "Contract", "Plan", "Name", "Change");
                    println!("{:-<10} {:-<10} {:-<40} {:-<10}", "", "", "", "");
                    if let Some(increases) = movers_json["increases"].as_array() {
                        for item in increases {
                            let cid = item[0].as_str().unwrap_or("");
                            let pid = item[1].as_str().unwrap_or("");
                            let name = item[2].as_str().unwrap_or("");
                            let change = item[3].as_i64().unwrap_or(0);
                            println!("{:<10} {:<10} {:<40} {:<10}", cid, pid, name, change);
                        }
                    }

                    println!("\nTOP DECREASES:");
                    println!("{:<10} {:<10} {:<40} {:<10}", "Contract", "Plan", "Name", "Change");
                    println!("{:-<10} {:-<10} {:-<40} {:-<10}", "", "", "", "");
                    if let Some(decreases) = movers_json["decreases"].as_array() {
                        for item in decreases {
                            let cid = item[0].as_str().unwrap_or("");
                            let pid = item[1].as_str().unwrap_or("");
                            let name = item[2].as_str().unwrap_or("");
                            let change = item[3].as_i64().unwrap_or(0);
                            println!("{:<10} {:<10} {:<40} {:<10}", cid, pid, name, change);
                        }
                    }

                    results_json = serde_json::json!({
                        "state": state,
                        "from": from,
                        "to": to,
                        "movers": movers_json
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
        None => {
            let port = 3000;
            log::info!("No command provided, defaulting to starting server on port {}", port);
            api::server::start_server(port, store_dir).await?;
        }
    }

    Ok(())
}
