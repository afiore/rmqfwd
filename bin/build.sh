#! /usr/bin/env bash

alias rust-musl-builder='docker run --rm -it -v "$(pwd)":/home/rust/src ekidd/rust-musl-builder'
rust-musl-builder cargo build --release --target x86_64-unknown-linux-musl
