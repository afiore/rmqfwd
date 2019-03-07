#! /usr/bin/env bash

docker run --rm -it -v "$(pwd)":/home/rust/src ekidd/rust-musl-builder sudo chown -R rust:rust src && cargo build --release --target x86_64-unknown-linux-musl
