use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "ma_store")]
#[command(about = "CMS Monthly Enrollment Hyper-Efficient Store", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Discover and download a specific month
    FetchMonth {
        /// Month in YYYY-MM format
        #[arg(short, long)]
        month: String,
        /// Force re-download even if already ingested
        #[arg(short, long)]
        force: bool,
    },
    /// Discover and download a range of months
    FetchRange {
        /// Start month in YYYY-MM format
        #[arg(short, long)]
        from: String,
        /// End month in YYYY-MM format
        #[arg(short, long)]
        to: String,
        /// Force re-download even if already ingested
        #[arg(short, long)]
        force: bool,
    },
    /// List all ingested months
    ListMonths,
    /// Validate the store integrity
    ValidateStore,
    /// Rebuild the high-speed binary cache
    RebuildCache,
    /// List some plans from the store
    ListPlans {
        /// Number of plans to list
        #[arg(short, long, default_value_t = 10)]
        limit: usize,
    },
    /// Query the store
    Query {
        #[command(subcommand)]
        query_command: QueryCommands,
    },
}

#[derive(Subcommand)]
pub enum QueryCommands {
    /// Query enrollment trend for a specific plan
    PlanTrend {
        #[arg(short, long)]
        contract: String,
        #[arg(short, long)]
        plan: String,
        #[arg(short, long)]
        state: Option<String>,
        #[arg(short = 'y', long)]
        county: Option<String>,
    },
    /// Snapshot of a county for a specific month
    CountySnapshot {
        #[arg(short, long)]
        state: String,
        #[arg(short, long)]
        county: String,
        #[arg(short, long)]
        month: String,
    },
}
