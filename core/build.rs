/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */

// Compiles the Prometheus Remote Write v1/v2 protos and the OpenNMS
// CollectionSet proto into Rust types via prost-build. The generated code is
// emitted into a single include file (`_protos.rs`) under OUT_DIR so that the
// empty-package CollectionSet types land at the include root and the namespaced
// Prometheus packages become nested modules.

use std::env;
use std::path::PathBuf;

fn main() {
    let manifest = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let proto_dir = PathBuf::from(&manifest).join("../proto");

    let protos = [
        proto_dir.join("types.proto"),
        proto_dir.join("remote.proto"),
        proto_dir.join("remote_v2.proto"),
        proto_dir.join("collectionset.proto"),
    ];

    // `collectionset.proto` imports `google/protobuf/wrappers.proto`. Some
    // protoc packages bundle the well-known types (e.g. Homebrew); others
    // (Debian/Ubuntu `protobuf-compiler`) ship them under a system include via
    // `libprotobuf-dev`. Add whichever include dir actually has them so the
    // build works on macOS, in CI, and in the container image alike.
    let mut includes: Vec<PathBuf> = vec![proto_dir.clone()];
    for candidate in [
        "/usr/include",
        "/usr/local/include",
        "/opt/homebrew/include",
    ] {
        let dir = PathBuf::from(candidate);
        if dir.join("google/protobuf/wrappers.proto").exists() {
            includes.push(dir);
        }
    }
    if includes.len() == 1 {
        // Only proto_dir; rely on protoc bundling the well-known types. If it
        // doesn't, compile_protos fails with a cryptic "wrappers.proto not
        // found" — surface the actionable fix up front.
        println!(
            "cargo:warning=google/protobuf/wrappers.proto not found under /usr/include, \
             /usr/local/include, or /opt/homebrew/include; if protoc lacks the bundled \
             well-known types the build will fail — install libprotobuf-dev (Debian/Ubuntu) \
             or the protobuf package (Homebrew)."
        );
    }

    let mut config = prost_build::Config::new();
    config.include_file("_protos.rs");
    config
        .compile_protos(&protos, &includes)
        .expect("failed to compile protobuf definitions");

    println!("cargo:rerun-if-changed={}", proto_dir.display());
}
