// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

fn main() {
    println!("cargo:rerun-if-changed=../../proto/flux_wire.proto");
    let mut config = prost_build::Config::new();
    config
        .compile_protos(&["../../proto/flux_wire.proto"], &["../../proto"])
        .expect("compile protobuf schema");
}
