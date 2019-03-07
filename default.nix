with import <nixpkgs> {};

stdenv.mkDerivation {
    name = "rmqfwd";

    buildInputs = with pkgs; [
      curl
      python
      rustup
      fzf
      jq
      psmisc
      git
      gnused
      coreutils
      travis
      musl
    ];

    RUST_LOG="warn";
}
