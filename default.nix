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
      #gitAndTools.hub
    ];

    RUST_LOG="warn";
}
