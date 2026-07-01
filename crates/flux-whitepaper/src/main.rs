// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use typst_as_lib::TypstEngine;

#[derive(Debug, Parser)]
#[command(
    name = "flux-whitepaper",
    about = "Compile Flux Typst whitepapers to PDF"
)]
struct Args {
    /// Input Typst document.
    #[arg(
        long,
        default_value = "crates/flux-whitepaper/typst/eventbus_whitepaper.typ"
    )]
    input: PathBuf,
    /// Output PDF path.
    #[arg(
        long,
        default_value = "crates/flux-whitepaper/out/eventbus_whitepaper.pdf"
    )]
    output: PathBuf,
    /// Root directory for relative file resolution inside Typst.
    #[arg(long)]
    root: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    compile_typst(&args)
}

fn compile_typst(args: &Args) -> Result<()> {
    let source = fs::read_to_string(&args.input)
        .with_context(|| format!("failed to read Typst source at {}", args.input.display()))?;

    let root = args
        .root
        .clone()
        .or_else(|| args.input.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| PathBuf::from("."));

    let engine = TypstEngine::builder()
        .main_file(source)
        .with_file_system_resolver(&root)
        .search_fonts_with(Default::default())
        .build();

    let warned = engine.compile();
    if !warned.warnings.is_empty() {
        eprintln!(
            "typst emitted {} warning(s) while compiling {}",
            warned.warnings.len(),
            args.input.display()
        );
    }
    let doc = warned
        .output
        .context("Typst compilation failed with no document output")?;

    let pdf_bytes = typst_pdf::pdf(&doc, &Default::default()).map_err(|diagnostics| {
        anyhow!(
            "failed to render Typst document to PDF ({} diagnostic(s))",
            diagnostics.len()
        )
    })?;

    if let Some(parent) = args.output.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output directory {}", parent.display()))?;
    }
    fs::write(&args.output, pdf_bytes)
        .with_context(|| format!("failed to write PDF to {}", args.output.display()))?;

    println!("Wrote {}", args.output.display());
    Ok(())
}
