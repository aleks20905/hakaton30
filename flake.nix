{
  description = "Curtis Production Dashboard";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
    in
    {
      packages.${system}.default = pkgs.stdenv.mkDerivation {
        pname = "curtisDashboard";
        version = "0.1.0";
        src = ./.;

        installPhase = ''
          mkdir -p $out
          cp app.py analytics_helpers.py $out/
          cp -r templates $out/
          cp -r static $out/
        '';
      };

      devShells.${system}.default = pkgs.mkShell {
        buildInputs = [
          (pkgs.python3.withPackages (ps: with ps; [
            flask
            pandas
            openpyxl
            xlrd
            gunicorn
          ]))
        ];
        shellHook = ''
          echo "✅ Curtis Dashboard dev environment ready"
          echo FLASK_DEBUG=true python app.py
        '';
      };

      nixosModules.default = import ./module.nix self;
    };
}
