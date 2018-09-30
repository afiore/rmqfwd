with import <nixpkgs> {};

stdenv.mkDerivation {
    name = "rust";
    buildInputs = [
        rustChannels.nightly.cargo
        rustChannels.nightly.rust
    ];
}
