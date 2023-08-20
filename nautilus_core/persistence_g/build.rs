// extern crate cbindgen;

use std::env;
use std::path::PathBuf;

#[allow(clippy::expect_used)] // OK in build script
fn main() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    // Generate C headers
    let config_c = cbindgen::Config::from_file("cbindgen.toml")
        .expect("unable to find cbindgen.toml configuration file");

    let c_header_path = crate_dir.join("../../nautilus_trader/core/includes/persistence_g.h");
    cbindgen::generate_with_config(&crate_dir, config_c)
        .expect("unable to generate bindings")
        .write_to_file(c_header_path);

    // Generate Cython definitions
    let config_cython = cbindgen::Config::from_file("cbindgen_cython.toml")
        .expect("unable to find cbindgen_cython.toml configuration file");

    let cython_path = crate_dir.join("../../nautilus_trader/core/rust/persistence_g.pxd");
    cbindgen::generate_with_config(&crate_dir, config_cython)
        .expect("unable to generate bindings")
        .write_to_file(cython_path);
}