#!/bin/bash
# Smoke-test the final runtime image as its default non-root user before CI publishes it.
# Check runtime identity/environment, tools and libraries, C++ compile/link/run,
# HPCC CLI startup, and SBOM JSON parsing; image construction alone cannot verify these.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${SCRIPT_DIR}/container-engine.sh"

usage() {
    echo "Usage: $0 -i <runtime-image>" >&2
}

IMAGE=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -i|--image) IMAGE="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) usage; exit 1 ;;
    esac
done

[[ -n "${IMAGE}" ]] || { usage; exit 1; }

container_run --rm "${IMAGE}" '
    set -euo pipefail
    test "$(id -u)" = 10000
    test "$(id -g)" = 10001
    test "$PWD" = /var/lib/HPCCSystems
    test "$HPCC_containerized" = 1
    test "$LC_ALL" = en_US.UTF-8
    test "$R_LIBS_SITE" = /usr/local/lib/R/site-library
    test ! -e /sbin/apk
    test ! -e /usr/sbin/apk
    for command in git git-lfs kubectl java python3 Rscript ssh rsync jq curl; do
        command -v "$command" >/dev/null
    done
    git --version
    git-lfs version
    kubectl version --client=true
    gcc --version
    g++ --version
    link_test_dir=$(mktemp -d)
    printf "int main() { return 0; }\n" > "$link_test_dir/eclcc-link-test.cpp"
    g++ "$link_test_dir/eclcc-link-test.cpp" -o "$link_test_dir/eclcc-link-test"
    "$link_test_dir/eclcc-link-test"
    rm -rf "$link_test_dir"
    java -version
    python3 --version
    Rscript -e "library(Rcpp); library(RInside); library(inline)"
    ssh -V
    jq --version
    curl --version
    test -n "$(find /usr/lib -name "libjemalloc.so*" -type f -print -quit)"
    test -x /opt/HPCCSystems/bin/ecl
    test -x /opt/HPCCSystems/bin/eclcc
    /opt/HPCCSystems/bin/ecl deploy --help >/dev/null
    if /opt/HPCCSystems/bin/eclcc --help >/dev/null; then
        :
    else
        status=$?
        test "$status" -eq 1
    fi
    python3 -c "import glob, json; [json.load(open(path)) for path in glob.glob(\"/var/lib/db/sbom/*.spdx.json\")]"
'
