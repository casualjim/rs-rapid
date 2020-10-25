fn main() {
  tonic_build::configure()
    .type_attribute(".remoting.NodeId", "#[derive(Ord, PartialOrd, Eq, Hash)]")
    .type_attribute(".remoting.Endpoint", "#[derive(Ord, PartialOrd, Eq, Hash)]")
    .compile(&["proto/remoting.proto"], &["proto"])
    .unwrap();
  println!("cargo:rerun-if-changed=proto/remoting.proto");
}
