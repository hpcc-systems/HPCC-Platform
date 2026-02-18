#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

show_help() {
    cat <<'EOF'
Usage: ./launch.sh [build|start|clean|build-start] [target-url]

Commands:
  build         Install deps and build (no server)
  clean         Remove build artifacts
  start         Start the dev server
  build-start   Install deps, build, then start (default)

Options:
  -h, --help    Show this help text

Target URL:
  http://localhost:8010
  https://play.hpccsystems.com:18010
EOF
}

command="build-start"
target=""
invalid_args=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            show_help
            exit 0
            ;;
        build|start|clean|build-start)
            command="$1"
            shift
            ;;
        http://*|https://*)
            if [[ -n "$target" ]]; then
                invalid_args=true
                break
            fi
            target="$1"
            shift
            ;;
        *)
            invalid_args=true
            break
            ;;
    esac
done

if [[ "$invalid_args" == true ]]; then
    show_help
    exit 1
fi

if [[ -z "$target" ]]; then
    target="https://play.hpccsystems.com:18010"
fi

if [[ "$command" == "clean" ]]; then
    npm run clean
    exit 0
fi

if [[ "$command" == "build" || "$command" == "build-start" ]]; then
    npm i
    npm run build-dev
fi

if [[ "$command" == "start" || "$command" == "build-start" ]]; then
    LWS_TARGET="$target" npm run dev-start-ws
fi
