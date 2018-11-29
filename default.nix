with import <nixpkgs> {};

stdenv.mkDerivation {
    name = "rmqfwd";

    buildInputs = [
      pkgs.curl
      pkgs.python
      pkgs.rustc
      pkgs.cargo
      pkgs.fzf
      pkgs.jq
      pkgs.psmisc
    ];

    RUST_LOG="warn";
}
