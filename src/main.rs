mod cli;
mod model;
mod cms;
mod ingest;
mod storage;
mod query;
mod util;

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
                let series_path = store_dir.join("facts").join(format!("year={}", month.year)).join(format!("enrollment_{}.parquet", month.month));
                if series_path.exists() {
                    println!("- {}: Series file exists", month);
                } else {
                    println!("- {}: Series file MISSING at {}", month, series_path.display());
                }
            }
        }
        Commands::RebuildCache => {
            log::info!("Rebuilding cache");
            // TODO: Implement rebuild_cache
        }
        Commands::ListPlans { limit } => {
            let plan_dim_path = store_dir.join("dims").join("plan_dim.parquet");
            let plans = storage::parquet_store::load_plan_dim(&plan_dim_path)?;
            println!("Listing first {} plans:", limit);
            for plan in plans.iter().take(limit) {
                println!("{}|{}: {} (Key: {})", plan.contract_id, plan.plan_id, plan.plan_name, plan.plan_key);
            }
        }
        Commands::Query { query_command } => {
            let engine = query::read_api::QueryEngine::new(store_dir);

            match query_command {
                cli::QueryCommands::PlanTrend { contract, plan, state, county } => {
                    log::info!("Querying plan trend: contract: {}, plan: {}, state: {:?}, county: {:?}", contract, plan, state, county);
                    
                    if let Some(plan_key) = engine.get_plan_key(&contract, &plan)? {
                        let trend = engine.get_plan_trend(plan_key)?;
                        println!("Trend for {}|{}:", contract, plan);
                        for (month, enrollment) in trend {
                            println!("{}: {}", month, enrollment);
                        }
                    } else {
                        println!("Plan not found: {}|{}", contract, plan);
                    }
                }
                cli::QueryCommands::CountySnapshot { state, county, month } => {
                    log::info!("Querying county snapshot: state: {}, county: {}, month: {}", state, county, month);
                    let month: model::YearMonth = month.parse()?;
                    
                    if let Some(county_key) = engine.get_county_key(&state, &county)? {
                        let snapshot = engine.get_county_snapshot(county_key, month)?;
                        println!("Snapshot for {}, {} in {}:", county, state, month);
                        println!("{:<10} {:<10} {:<40} {:<10}", "Contract", "Plan", "Name", "Enrollment");
                        println!("{:-<10} {:-<10} {:-<40} {:-<10}", "", "", "", "");
                        for (cid, pid, name, enrollment) in snapshot {
                            println!("{:<10} {:<10} {:<40} {:<10}", cid, pid, name, enrollment);
                        }
                    } else {
                        println!("County not found: {}, {}", county, state);
                    }
                }
            }
        }
    }

    Ok(())
}
