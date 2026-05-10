{
  description = "Curtis Production Dashboard";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs =
    { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
      pythonEnv = pkgs.python3.withPackages (
        ps: with ps; [
          flask
          pandas
          openpyxl
          xlrd
          gunicorn
        ]
      );
    in
    {
      packages.${system}.default = pkgs.stdenv.mkDerivation {
        pname = "curtisDashboard";
        version = "0.1.0";
        src = ./.;

        buildInputs = [ pythonEnv ];
        doCheck = true;
        checkPhase = ''
            export SECRET_KEY=build-time-test-key
            export PYTHONPATH=.:$PYTHONPATH
            python3 -m unittest discover -s tests -t .
        '';

        installPhase = ''
          mkdir -p $out
          cp app.py analytics_helpers.py $out/
          cp -r templates $out/
          cp -r static $out/
        '';
      };

      devShells.${system}.default = pkgs.mkShell {
        buildInputs = [ pythonEnv ];
        shellHook = ''
          venv="$(cd $(dirname $(which python)); cd ..; pwd)"
          ln -Tsf "$venv" .venv

          echo "✅ Curtis Dashboard dev environment ready"
          echo FLASK_DEBUG=true python app.py
        '';
      };

      nixosModules.default = import ./module.nix self;
    };
}
