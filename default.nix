with import <nixpkgs> {};


stdenv.mkDerivation {
    name = "rmqfwd";

    buildInputs = [
      pkgs.curl
      pkgs.python
      pkgs.rustc
      pkgs.cargo
    ];
}
