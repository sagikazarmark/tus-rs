{
  pkgs,
  config,
  lib,
  ...
}:

let
  cfg = config.languages.dang;

  package = pkgs.buildGoModule rec {
    name = "dang";
    version = "4cbbcacc8bd52d941b058602d7af08a68e469ba9";

    src = pkgs.fetchFromGitHub {
      owner = "vito";
      repo = "dang";
      # rev = "v${version}";
      rev = "${version}";
      sha256 = "sha256-huaAP8aT/qtAKvEm4/p81LJxKhJ7/LuhvfZWSJOPiV4=";
    };

    vendorHash = "sha256-yV6ubM93VyXTVRuqmPAluj+HaCOSnwk7FGmpSG33l5s=";
    proxyVendor = true;

    doCheck = false;
    subPackages = [ "cmd/dang" ];

    meta = with lib; {
      description = "Experimental GraphQL scripting language";
      homepage = "https://github.com/vito/dang";
      license = licenses.asl20;
      platforms = platforms.unix;
    };
  };
in
{
  options.languages.dang = {
    enable = lib.mkEnableOption "tools for Dang development";

    package = lib.mkOption {
      type = lib.types.package;
      default = package;
      defaultText = lib.literalExpression "pkgs.dang";
      description = "The Dang package to use.";
    };
  };

  config = lib.mkIf cfg.enable {
    packages = [
      cfg.package
    ];
  };
}
