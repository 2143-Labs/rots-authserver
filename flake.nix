{
  description = "The auth and central game server for ROTS. needs postgres backend env vars";

  inputs = {
    naersk.url = "github:nix-community/naersk/master";
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, utils, naersk }:
    utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        naersk-lib = pkgs.callPackage naersk { };
        buildInputsAll = with pkgs; [
          udev
        ];
        # Server package - headless, doesn't need graphics libraries
        serverPackage = naersk-lib.buildPackage {
          src = ./.;
          nativeBuildInputs = with pkgs; [ pkg-config ];
          buildInputs = with pkgs; [
            udev
          ];
        };
      in
      rec {
        # Default package is the client
        packages.default = serverPackage;
        packages.server = serverPackage;
        packages.container = pkgs.dockerTools.buildLayeredImage {
          name = "rots-authserver";
          tag = "latest";
          contents = [
            serverPackage
            pkgs.cacert
            pkgs.bashInteractive
            pkgs.coreutils
          ];
          config = {
            ExposedPorts = { "8000/udp" = { }; };
            EntryPoint = [ "${serverPackage}/bin/server" ];
            Env = [
              "RUST_LOG=info"
            ];
            # Add labels for better container metadata
            Labels = {
              "org.opencontainers.image.source" = "https://github.com/2143-Labs/rots-authserver";
              "org.opencontainers.image.description" = "ROTS Auth Server";
            };
          };
        };

        devShells.default = with pkgs; mkShell {
          buildInputs = [
            rust-analyzer
            cargo
            rustPackages.rustfmt
            rustPackages.clippy
            cargo-flamegraph
            pre-commit
            pkg-config
            bacon
            # Additional useful development tools
            cargo-audit
            cargo-deny
            cargo-outdated
            nixpkgs-fmt
            # lld is specifically required by the wasm compiler for web builds (tracing-wasm)
            lld
            binaryen
            sqlx-cli
          ] ++ buildInputsAll;
          RUST_SRC_PATH = rustPlatform.rustLibSrc;
          LD_LIBRARY_PATH = lib.makeLibraryPath buildInputsAll;
          # Set environment variables for better development experience
          shellHook = ''
            echo "Bevy 2025 development environment"
          '';
        };
      }
    );
}
