self: { config, lib, pkgs, ... }:

let
  cfg = config.services.curtisDashboard;

  pythonEnv = pkgs.python3.withPackages (ps: with ps; [
    flask
    pandas
    openpyxl
    gunicorn
  ]);

  appPkg = self.packages.${pkgs.system}.default;

in
{
  options.services.curtisDashboard = {
    enable = lib.mkEnableOption "Curtis Production Dashboard";

    port = lib.mkOption {
      type = lib.types.port;
      default = 8000;
      description = "Port to listen on";
    };

    dataDir = lib.mkOption {
      type = lib.types.str;
      default = "/var/lib/curtisDashboard";
      description = "Directory to store the xlsx data file";
    };

    openFirewall = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Open the firewall for the dashboard port";
    };

    secretKeyFile = lib.mkOption {
      type = lib.types.str;
      description = "Path to a file containing the Flask SECRET_KEY";
      example = "/etc/curtisDashboard/secrets";
    };
  };

  config = lib.mkIf cfg.enable {

    systemd.tmpfiles.rules = [
      "d '${cfg.dataDir}' 0750 curtisDashboard curtisDashboard -"
    ];

    systemd.services.curtisDashboard = {
      description = "Curtis Production Dashboard";
      after = [ "network.target" ];
      wantedBy = [ "multi-user.target" ];

      environment = {
        DATA_DIR = cfg.dataDir;
        FLASK_DEBUG = "false";
      };

      serviceConfig = { 
        ExecStart = "${pythonEnv}/bin/gunicorn -w 4 -b 0.0.0.0:${toString cfg.port} app:app";
        WorkingDirectory = "${appPkg}";
        EnvironmentFile = cfg.secretKeyFile;
        Restart = "always";
        User = "curtisDashboard";
        Group = "curtisDashboard";
        NoNewPrivileges = true;
        PrivateTmp = true;
      };
    };

    users.users.curtisDashboard = {
      isSystemUser = true;
      group = "curtisDashboard";
      description = "Curtis Dashboard service user";
    };

    users.groups.curtisDashboard = {};

    networking.firewall.allowedTCPPorts = lib.mkIf cfg.openFirewall [ cfg.port ];
  };
}
