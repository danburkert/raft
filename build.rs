#![feature(old_path)]

extern crate capnpc;

fn main() {
    ::capnpc::compile(Path::new("src"), &[Path::new("src/messages.capnp")]).unwrap();
}

