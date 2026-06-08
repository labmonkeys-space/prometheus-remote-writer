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

    let mut config = prost_build::Config::new();
    config.include_file("_protos.rs");
    config
        .compile_protos(&protos, &[&proto_dir])
        .expect("failed to compile protobuf definitions");

    println!("cargo:rerun-if-changed={}", proto_dir.display());
}
