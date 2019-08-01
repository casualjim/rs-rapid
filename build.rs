fn main() {
  let mut prost_config = prost_build::Config::new();
  prost_config.type_attribute(".remoting.NodeId", "#[derive(Ord, PartialOrd, Eq, Hash)]");
  prost_config.type_attribute(".remoting.Endpoint", "#[derive(Ord, PartialOrd, Eq, Hash)]");
  prost_config
    .compile_protos(&["proto/remoting.proto"], &["proto"])
    .unwrap_or_else(|e| panic!("protobuf compilation failed: {}", e));
  //  grpcio_compiler::prost_codegen::compile_protos(&["proto/remoting.proto"], &["proto"], "src")
  //    .unwrap_or_else(|e| panic!("grpc compilation failed: {}", e));

  // tower_grpc_build::Config::from_prost(prost_config)
  //   .enable_server(true)
  //   .enable_client(true)
  //   .build(&["proto/remoting.proto"], &["proto"])
  //   .unwrap_or_else(|e| panic!("protobuf compilation failed: {}", e));
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
