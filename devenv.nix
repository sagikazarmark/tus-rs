{ pkgs, ... }:

{
  imports = [
    ./nix/dang.nix
    ./nix/dagger.nix
  ];

  dotenv.enable = true;

  packages = with pkgs; [
    lld
    cargo-audit
    cargo-deny
    cargo-dist
    cargo-release
    cargo-watch
  ];

  dagger = {
    enable = true;
  };

  languages = {
    rust = {
      enable = true;
    };
  };
}
