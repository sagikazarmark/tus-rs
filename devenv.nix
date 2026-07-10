{ pkgs, ... }:

{
  dotenv.enable = true;

  dagger.enable = true;
  env.DAGGER_X_RELEASE = "86d1d2f5791bcf3213d56903cfa81a3ba0abe54a";

  # Cross-compile the wasm32 C sysroot (pulled in by the `dioxus-code` snippet
  # highlighter via arborium/tree-sitter) with the *unwrapped* clang. The
  # Nix cc-wrapper injects `-fzero-call-used-regs=used-gpr` from its hardening
  # set, which clang rejects for the wasm32 target and breaks `cargo build`
  # /`cargo clippy --target wasm32-unknown-unknown` in the demo.
  env.CC_wasm32_unknown_unknown = "${pkgs.llvmPackages.clang-unwrapped}/bin/clang";

  packages = with pkgs; [
    lld
    cargo-audit
    cargo-deny
    cargo-dist
    cargo-release
    cargo-watch
    wasm-pack
  ];

  languages = {
    rust = {
      enable = true;
      channel = "stable";
      targets = [ "wasm32-unknown-unknown" ];
    };
    javascript = {
      enable = true;
      npm.enable = true;
      bun.enable = true;
    };
  };
}
