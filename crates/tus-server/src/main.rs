//! TUS Resumable Upload Server
//!
//! A standalone server implementing the TUS protocol for resumable file uploads.

mod app;
mod command;
mod config;
mod expiration;
mod lifecycle;

use clap::Parser;

use crate::config::{Cli, Command};

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        match cli.command {
            Command::Serve(command) => command::run_serve(*command).await,
            Command::Cleanup(command) => command::run_cleanup(command).await,
        }
    })
}
