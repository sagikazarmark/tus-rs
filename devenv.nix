{ pkgs, ... }:

{
  dotenv.enable = true;

  packages = with pkgs; [
    cargo-audit
    cargo-deny
    cargo-release
    cargo-watch
    dagger
  ];

  languages = {
    rust = {
      enable = true;
    };
  };
}
