fn main() {
  tower_grpc_build::Config::new()
    .enable_server(true)
    .enable_client(true)
    .build(
      &["proto/remoting.proto"],
      &["proto"],
    ).unwrap_or_else(|e| panic!("protobuf compilation failed: {}", e));
  println!("cargo:rerun-if-changed=proto/remoting.proto");

//    protoc_rust_grpc::run(protoc_rust_grpc::Args {
//        out_dir: "src",
//        includes: &["proto"],
//        input: &[
//            "proto/remoting.proto",
//        ],
//        rust_protobuf: true,
//        ..Default::default()
//    }).expect("protoc-rust-grpc");
}
