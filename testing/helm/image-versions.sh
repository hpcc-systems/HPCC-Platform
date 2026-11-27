#!/bin/bash
set -euo pipefail

scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
chart=$(mktemp -d)
trap 'rm -rf "$chart"' EXIT
mkdir "$chart/templates"
cp "$scriptdir/../../helm/hpcc/templates/_helpers.tpl" "$chart/templates/"
cat > "$chart/Chart.yaml" <<'EOF'
apiVersion: v2
name: image-version-test
version: 1.2.3
EOF
cat > "$chart/values.yaml" <<'EOF'
global:
  image: {}
component: {}
EOF
cat > "$chart/templates/image.yaml" <<'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: image-version-test
data:
  image: {{ include "hpcc.imageName" (dict "root" . "me" .Values.component) | quote }}
EOF

check_image() {
    local expected=$1
    shift
    local output
    output=$(helm template "$chart" "$@")
    if ! grep -Fxq "  image: \"$expected\"" <<< "$output"; then
        echo "Expected image: $expected" >&2
        echo "$output" >&2
        exit 1
    fi
}

digest=sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
other_digest=sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789
repository=hpccsystems/platform-core

check_image "$repository:1.2.3"
check_image "$repository:1.2.3" --set-string global.image.version= --set-string component.image.name=platform-core
check_image "$repository:latest" --set-string global.image.version=latest
check_image "$repository@$digest" --set-string global.image.version="$digest"
check_image "$repository@$digest" --set-string global.image.version="$digest" --set-string component.image.name=platform-core
check_image "$repository@$digest" --set-string global.image.version="$digest" --set-string component.image.version= --set-string component.image.name=platform-core
check_image "$repository@$digest" --set-string component.image.version="$digest"
check_image "$repository@$other_digest" --set-string global.image.version="$digest" --set-string component.image.version="$other_digest"
check_image "$repository:10.8.0" --set-string global.image.version="$digest" --set-string component.image.version=10.8.0
check_image "$repository@$digest" --set-string global.image.version=10.8.0 --set-string component.image.version="$digest"
check_image "registry.example:5000/custom@$digest" --set-string global.image.root=registry.example:5000 --set-string global.image.name=custom --set-string global.image.version="$digest"
check_image "component.example/custom@$digest" --set-string component.image.root=component.example --set-string component.image.name=custom --set-string component.image.version="$digest"
check_image "$repository:latest" --set-string global.image.version=latest --set-string component.image.name=platform-core
check_image "$repository:123" --values /dev/stdin <<< 'global: {image: {version: 123}}'
check_image "$repository:123" --set-string component.image.name=platform-core --values /dev/stdin <<< 'global: {image: {version: 123}}'
check_image "$repository:123" --set-string global.image.version="$digest" --values /dev/stdin <<< 'component: {image: {version: 123}}'

for invalid in "${digest%?}" "${digest}0" "${digest%?}F" "${digest%?}g" "SHA256:${digest#sha256:}"; do
    check_image "$repository:$invalid" --set-string global.image.version="$invalid"
    check_image "$repository:$invalid" --set-string component.image.version="$invalid"
done
echo "Image version rendering tests passed (26 cases)"
