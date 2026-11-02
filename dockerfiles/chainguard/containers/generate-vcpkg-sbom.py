#!/usr/bin/env python3
"""Create file-level vcpkg provenance for files present in a staged runtime."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import quote
from uuid import uuid4

GITHUB_SOURCE = re.compile(r"^git\+https://github\.com/([^/]+)/([^/@]+?)(?:\.git)?@(.+)$")
DIGEST_LENGTHS = {"SHA1": 40, "SHA256": 64, "SHA512": 128}


def digests(path):
    sha1 = hashlib.sha1()
    sha256 = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            sha1.update(block)
            sha256.update(block)
    return sha1.hexdigest(), sha256.hexdigest()


# SPDX 2.3 requires a SHA1 for every file and a verification code for every package with filesAnalyzed.
def file_checksums(sha1, sha256):
    return [{"algorithm": "SHA1", "checksumValue": sha1}, {"algorithm": "SHA256", "checksumValue": sha256}]


def verification_code(sha1s):
    return {"packageVerificationCodeValue": hashlib.sha1("".join(sorted(sha1s)).encode()).hexdigest()}


def source_purl(download_location):
    match = GITHUB_SOURCE.match(download_location)
    # Some portfiles record the ref unexpanded, e.g. curl's "${curl_version}".
    if not match or "${" in match.group(3):
        return None
    owner, repo, ref = match.groups()
    return f"pkg:github/{owner.lower()}/{repo.lower()}@{quote(ref, safe='')}"


# vcpkg emits LicenseRef-vcpkg-null etc. without the extracted licensing info SPDX requires for them.
def license(value):
    return "NOASSERTION" if not value or "LicenseRef-" in value else value


def spdx_file_name(stage_root, path):
    return "./" + path.relative_to(stage_root).as_posix()


def valid_checksum(value):
    length = DIGEST_LENGTHS.get(value.get("algorithm"))
    return bool(length) and re.fullmatch(f"[0-9a-f]{{{length}}}", value.get("checksumValue", "")) is not None


def checksum(entry):
    for value in entry.get("checksums", []):
        if value.get("algorithm") == "SHA256":
            return value.get("checksumValue", "").lower()
    return ""


def package_for_document(document):
    packages = document.get("packages", [])
    for package in packages:
        if package.get("SPDXID") == "SPDXRef-port":
            return package
    return packages[0] if packages else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage-root", required=True, type=Path)
    parser.add_argument("--vcpkg-packages-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--hpcc-package-name", required=True)
    parser.add_argument("--hpcc-package-version", required=True)
    args = parser.parse_args()

    staged_by_hash = {}
    staged_by_path = {}
    staged_digests = {}
    for path in args.stage_root.rglob("*"):
        if path.is_file() and not path.is_symlink():
            staged_digests[path] = digests(path)
            staged_by_hash.setdefault(staged_digests[path][1], []).append(path)
            staged_by_path["/" + path.relative_to(args.stage_root).as_posix()] = path

    hpcc_package_id = "SPDXRef-HpccPackage"
    packages = []
    files = []
    relationships = []
    package_index = 0
    file_index = 0
    source_index = 0
    vcpkg_staged_paths = set()
    documents = []
    for source in sorted(args.vcpkg_packages_dir.glob("*/share/*/vcpkg.spdx.json")):
        document = json.loads(source.read_text())
        port = package_for_document(document)
        if not port:
            continue
        documents.append((source, port, document))

    matched_by_source = {source: [] for source, _, _ in documents}
    for match_paths in (True, False):
        for source, _, document in documents:
            for entry in document.get("files", []):
                digest = checksum(entry)
                if match_paths:
                    staged_paths = [staged_by_path[entry["fileName"]]] if entry.get("fileName") in staged_by_path else []
                else:
                    staged_paths = staged_by_hash.get(digest, []) if len(staged_by_hash.get(digest, [])) == 1 else []
                for staged_path in staged_paths:
                    if staged_path not in vcpkg_staged_paths:
                        vcpkg_staged_paths.add(staged_path)
                        matched_by_source[source].append((entry, staged_path))

    for source, port, document in documents:
        matched = matched_by_source[source]
        if not matched:
            continue

        package_id = f"SPDXRef-VcpkgPackage-{package_index}"
        package_index += 1
        package = {
            "name": port.get("name", source.parent.parent.parent.name),
            "SPDXID": package_id,
            "versionInfo": port.get("versionInfo", "NOASSERTION"),
            "downloadLocation": port.get("downloadLocation", "NOASSERTION"),
            "filesAnalyzed": True,
            "packageVerificationCode": verification_code(staged_digests[path][0] for _, path in matched),
            "licenseConcluded": license(port.get("licenseConcluded")),
            "licenseDeclared": license(port.get("licenseDeclared")),
            "copyrightText": port.get("copyrightText", "NOASSERTION"),
        }
        for key in ("homepage", "description"):
            if key in port:
                package[key] = port[key]
        packages.append(package)
        relationships.append({
            "spdxElementId": hpcc_package_id,
            "relationshipType": "DEPENDS_ON",
            "relatedSpdxElement": package_id,
        })

        # Upstream archives/repositories the port was built from; their purls let scanners match CVEs.
        for resource in document.get("packages", []):
            if not resource.get("SPDXID", "").startswith("SPDXRef-resource"):
                continue
            source_id = f"SPDXRef-VcpkgSource-{source_index}"
            source_index += 1
            source_package = {
                **resource,
                "SPDXID": source_id,
                "filesAnalyzed": False,
                "licenseConcluded": license(resource.get("licenseConcluded")),
                "licenseDeclared": license(resource.get("licenseDeclared")),
                # Some portfiles leave placeholder hashes such as "0" or "${expected_sha}".
                "checksums": [value for value in resource.get("checksums", []) if valid_checksum(value)],
            }
            if not source_package["checksums"]:
                del source_package["checksums"]
            purl = source_purl(resource.get("downloadLocation", ""))
            if purl:
                source_package["externalRefs"] = [{
                    "referenceCategory": "PACKAGE-MANAGER",
                    "referenceType": "purl",
                    "referenceLocator": purl,
                }]
            packages.append(source_package)
            relationships.append({
                "spdxElementId": package_id,
                "relationshipType": "GENERATED_FROM",
                "relatedSpdxElement": source_id,
            })

        for entry, staged_path in matched:
            file_id = f"SPDXRef-VcpkgFile-{file_index}"
            file_index += 1
            files.append({
                "fileName": spdx_file_name(args.stage_root, staged_path),
                "SPDXID": file_id,
                "checksums": file_checksums(*staged_digests[staged_path]),
                "licenseConcluded": license(entry.get("licenseConcluded")),
                "copyrightText": entry.get("copyrightText", "NOASSERTION"),
            })
            relationships.append({
                "spdxElementId": package_id,
                "relationshipType": "CONTAINS",
                "relatedSpdxElement": file_id,
            })

    hpcc_paths = [path for path in staged_digests if path not in vcpkg_staged_paths]
    packages.append({
        "name": args.hpcc_package_name,
        "SPDXID": hpcc_package_id,
        "versionInfo": args.hpcc_package_version,
        "downloadLocation": "NOASSERTION",
        "filesAnalyzed": True,
        "packageVerificationCode": verification_code(staged_digests[path][0] for path in hpcc_paths),
        "licenseConcluded": "Apache-2.0",
        "licenseDeclared": "Apache-2.0",
        "copyrightText": "NOASSERTION",
    })
    relationships.append({
        "spdxElementId": "SPDXRef-DOCUMENT",
        "relationshipType": "DESCRIBES",
        "relatedSpdxElement": hpcc_package_id,
    })
    for staged_path in hpcc_paths:
        file_id = f"SPDXRef-HpccFile-{file_index}"
        file_index += 1
        files.append({
            "fileName": spdx_file_name(args.stage_root, staged_path),
            "SPDXID": file_id,
            "checksums": file_checksums(*staged_digests[staged_path]),
            "licenseConcluded": "NOASSERTION",
            "copyrightText": "NOASSERTION",
        })
        relationships.append({
            "spdxElementId": hpcc_package_id,
            "relationshipType": "CONTAINS",
            "relatedSpdxElement": file_id,
        })

    result = {
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "SPDXID": "SPDXRef-DOCUMENT",
        "name": f"{args.hpcc_package_name}-{args.hpcc_package_version} runtime vcpkg payload provenance",
        "documentNamespace": f"https://hpccsystems.com/spdx/{args.hpcc_package_name}-{args.hpcc_package_version}/runtime-vcpkg/{uuid4()}",
        "creationInfo": {
            "created": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "creators": ["Tool: hpcc-runtime-vcpkg-provenance"],
        },
        "documentDescribes": [hpcc_package_id],
        "packages": packages,
        "files": files,
        "relationships": relationships,
    }
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(f"Matched {package_index} vcpkg packages ({source_index} upstream sources) and recorded {len(files)} staged files")


if __name__ == "__main__":
    main()