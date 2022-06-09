let
  pkgs =
    import (builtins.fetchTarball {
      name = "nixos-unstable-2022-06-08";
      url = "https://github.com/nixos/nixpkgs/archive/43ecbe7840d155fa933ee8a500fb00dbbc651fc8.tar.gz";
      sha256 = "1js0cbzy8xfp1wbshsaxjd45xzm3w8fvlf5z573fw4hrzajlkfgd";
    }) {};

in pkgs.runCommand "dummy" { buildInputs = with pkgs; [ reveal-md ]; } ""
