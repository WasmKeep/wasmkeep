#!/usr/bin/env bash
# Builds counter.wasm with componentize-dotnet: wit-bindgen's C# bindings
# for the actor world, then NativeAOT-LLVM to a WASI 0.2 component. The
# packages (about 800 MB, preview) restore from nuget.org and the
# dotnet-experimental feed (nuget.config).
#   .NET SDK 10: https://dotnet.microsoft.com/download/dotnet/10.0 (Linux x64 here; the compiler is also published for Linux arm64 and Windows x64)
#   cargo install wasm-tools
set -euo pipefail
cd "$(dirname "$0")"
DOTNET=${DOTNET:-dotnet}
export DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1
"$DOTNET" build -c Release -nologo -v quiet
# Without the debug sections NativeAOT-LLVM leaves: 2.5 MB down to 0.9 MB.
wasm-tools strip --all bin/Release/net10.0/wasi-wasm/publish/counter.wasm -o counter.wasm
echo "wrote $(pwd)/counter.wasm ($(wc -c < counter.wasm) bytes)"
