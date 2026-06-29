{ pkgs, ... }:

{
  dotenv.enable = true;

  dagger.enable = true;

  packages = with pkgs; [
    lld
    cargo-audit
    cargo-deny
    cargo-dist
    cargo-release
    cargo-watch
  ];

  languages = {
    rust = {
      enable = true;
    };
  };
}
