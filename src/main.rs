mod cli;
mod model;
mod cms;
mod util;

use clap::Parser;
use cli::{Cli, Commands};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logger
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();

    match cli.command {
        Commands::FetchMonth { month, force } => {
            log::info!("Fetching month: {}, force: {}", month, force);
            let month: model::YearMonth = month.parse()?;
            
            let source_info = cms::discover::discover_month(month).await?;
            log::info!("Discovered ZIP at: {}", source_info.zip_url);

            let zip_bytes = cms::download::download_zip(&source_info.zip_url).await?;
            let hash = util::hashing::compute_sha256(&zip_bytes);
            log::info!("Downloaded ZIP with hash: {}", hash);
            
            // TODO: Proceed to Parse / Normalize Layer
        }
        Commands::FetchRange { from, to, force } => {
            log::info!("Fetching range: from {} to {}, force: {}", from, to, force);
            // TODO: Implement fetch_range
        }
        Commands::ListMonths => {
            log::info!("Listing months");
            // TODO: Implement list_months
        }
        Commands::ValidateStore => {
            log::info!("Validating store");
            // TODO: Implement validate_store
        }
        Commands::RebuildCache => {
            log::info!("Rebuilding cache");
            // TODO: Implement rebuild_cache
        }
        Commands::Query { query_command } => {
            match query_command {
                cli::QueryCommands::PlanTrend { contract, plan, state, county } => {
                    log::info!("Querying plan trend: contract: {}, plan: {}, state: {:?}, county: {:?}", contract, plan, state, county);
                    // TODO: Implement plan_trend query
                }
                cli::QueryCommands::CountySnapshot { state, county, month } => {
                    log::info!("Querying county snapshot: state: {}, county: {}, month: {}", state, county, month);
                    // TODO: Implement county_snapshot query
                }
            }
        }
    }

    Ok(())
}
