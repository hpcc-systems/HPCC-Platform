#!/usr/bin/env python3

import argparse
import collections
import concurrent.futures
import html
import json
import os
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ElementTree


def local_name(tag):
    return tag.rsplit("}", 1)[-1]


def post(endpoint, action, request_name, content, timeout):
    body = (
        '<?xml version="1.0"?>'
        '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">'
        f'<soap:Body><{request_name} xmlns="urn:hpccsystems:ws:wssql2">'
        f"{content}</{request_name}></soap:Body></soap:Envelope>"
    )
    request = urllib.request.Request(
        endpoint,
        data=body.encode(),
        headers={"Content-Type": "text/xml; charset=utf-8", "SOAPAction": f'"{action}"'},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return ElementTree.fromstring(response.read())


def list_logical_files(endpoint, scope, timeout):
    files = []
    page_size = 500
    page_start = 1
    total_files = None
    while total_files is None or len(files) < total_files:
        query = urllib.parse.urlencode({
            "LogicalName": scope,
            "PageSize": page_size,
            "PageStartFrom": page_start,
        })
        request = urllib.request.Request(f"{endpoint}?{query}")
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read())
        if "DFUQueryResponse" in payload:
            payload = payload["DFUQueryResponse"]
        if "DFULogicalFiles" not in payload:
            raise ValueError("DFUQuery response did not include DFULogicalFiles")

        file_container = payload["DFULogicalFiles"]
        if isinstance(file_container, dict):
            page_files = file_container.get("DFULogicalFile", [])
        else:
            page_files = file_container
        if isinstance(page_files, dict):
            page_files = [page_files]
        if not isinstance(page_files, list):
            raise ValueError("DFUQuery returned DFULogicalFiles in an unexpected format")
        files.extend(item["Name"] for item in page_files if isinstance(item, dict) and item.get("Name"))

        if total_files is None:
            reported_total = payload.get("NumFiles")
            total_files = int(reported_total) if reported_total is not None else None
        if total_files is not None:
            if len(files) >= total_files:
                break
            if not page_files:
                raise ValueError("DFUQuery returned fewer logical files than NumFiles")
        elif len(page_files) < page_size:
            break
        page_start += len(page_files)
    return files


def classify(error):
    categories = (
        ("missing_layout", "could not determine a record layout"),
        ("nested_or_decimal", "does not support nested or decimal"),
        ("grouped", "group info. differs"),
        ("unsupported_format", "could not initialize the published record layout"),
        ("key_layout", "could not map KEY field"),
        ("decimal_conversion", "to DECIMAL"),
        ("offline_disabled", "offline overload is disabled"),
    )
    for category, marker in categories:
        if marker in error:
            return category
    return "other"


def response_error(root):
    return next(
        (" ".join("".join(node.itertext()).split()) for node in root.iter()
         if local_name(node.tag) == "Exception"),
        None,
    )


def response_text(root, name):
    return next(
        ("".join(node.itertext()).strip() for node in root.iter() if local_name(node.tag) == name),
        None,
    )


def response_columns(root):
    metadata = next((node for node in root.iter() if local_name(node.tag) == "ResultSetMetadata"), None)
    columns = []
    if metadata is not None:
        for column in metadata:
            fields = {local_name(field.tag): field for field in column}
            label = fields.get("ColumnLabel")
            columns.append("".join(label.itertext()).strip() if label is not None else "")
    return columns


def execute_sql(endpoint, sql, timeout):
    # ExecuteQuery only prepares the query and returns a cursor; FetchResultSet runs it and returns the rows.
    root = post(
        endpoint,
        "ExecuteQuery",
        "ExecuteQueryRequest",
        f"<SQLText>{html.escape(sql, quote=False)}</SQLText><IncludeResultSetMetadata>true</IncludeResultSetMetadata>",
        timeout,
    )
    error = response_error(root)
    if error:
        return error, None
    cursor_id = response_text(root, "CursorId")
    if not cursor_id:
        return "ExecuteQuery response is missing CursorId", None
    result = {"columns": response_columns(root), "rows": []}
    complete = False
    try:
        while not complete:
            root = post(
                endpoint,
                "FetchResultSet",
                "FetchResultSetRequest",
                f"<CursorId>{html.escape(cursor_id)}</CursorId>",
                timeout,
            )
            error = response_error(root)
            if error:
                return error, None
            rows = response_text(root, "RowsJSON")
            if rows is None:
                return "FetchResultSet response is missing RowsJSON", None
            result["rows"].extend(json.loads(rows))
            complete = (response_text(root, "Complete") or "").lower() in ("true", "1")
    finally:
        if not complete:
            post(
                endpoint,
                "CloseResultSet",
                "CloseResultSetRequest",
                f"<CursorId>{html.escape(cursor_id)}</CursorId>",
                timeout,
            )
    result["rowCount"] = len(result["rows"])
    result["returnedRowCount"] = len(result["rows"])
    result["truncated"] = False
    return None, result


def get_logical_file_meta(endpoint, logical_name, timeout):
    root = post(
        endpoint,
        "LogicalFileMetadata",
        "LogicalFileMetadataRequest",
        f"<LogicalName>{html.escape(logical_name)}</LogicalName>",
        timeout,
    )
    error = response_error(root)
    if error:
        return error, None
    meta = next((node for node in root.iter() if local_name(node.tag) == "LogicalFileMetadata"), None)
    if meta is None:
        return "LogicalFileMetadata response is missing its LogicalFileMetadata struct", None
    required_fields = {
        "Version", "LogicalName", "Kind", "Format", "Grouped", "FormatCrc",
        "RecordTranslationMode", "Modified", "FormatOptions", "ProviderOptions",
        "EclRecord", "FileParts", "SampleSQL",
    }
    fields = {local_name(node.tag): node for node in meta}
    missing_fields = sorted(required_fields.difference(fields))
    if missing_fields:
        return f"LogicalFileMetadata missing required fields: {', '.join(missing_fields)}", None

    scalar = lambda name: "".join(fields[name].itertext()).strip()
    try:
        parts = json.loads(scalar("FileParts"))
        format_options = json.loads(scalar("FormatOptions") or "{}")
        provider_options = json.loads(scalar("ProviderOptions") or "{}")
        grouped_text = scalar("Grouped").lower()
        if grouped_text not in ("true", "false", "1", "0"):
            raise ValueError("Grouped is not a boolean")
        options = {
            "@version": int(scalar("Version")),
            "@logicalName": scalar("LogicalName"),
            "@kind": scalar("Kind"),
            "@format": scalar("Format"),
            "@grouped": grouped_text in ("true", "1"),
            "@formatCrc": int(scalar("FormatCrc")),
            "@recordTranslationMode": int(scalar("RecordTranslationMode")),
            "@modified": int(scalar("Modified")),
            "formatOptions": format_options,
            "providerOptions": provider_options,
        }
        record_count = int(scalar("RecordCount")) if "RecordCount" in fields else None
        if record_count is not None and record_count < 0:
            raise ValueError("RecordCount must be non-negative when present")
    except json.JSONDecodeError as exception:
        return f"LogicalFileMetadata returned invalid JSON: {exception}", None
    except ValueError as exception:
        return f"LogicalFileMetadata returned invalid field values: {exception}", None
    if not isinstance(parts, dict) or not isinstance(format_options, dict) or not isinstance(provider_options, dict):
        return "LogicalFileMetadata parts and option objects have unexpected JSON types", None
    if "field" in provider_options or "FieldsJson" in scalar("ProviderOptions"):
        return "LogicalFileMetadata provider options include redundant field descriptors", None
    parts_json = json.dumps(parts, separators=(",", ":"))
    options_json = json.dumps(options, separators=(",", ":"))
    sample_sql = scalar("SampleSQL")
    if not sample_sql.startswith("SELECT *\nFROM hpcc_scan(\n"):
        return "LogicalFileMetadata returned an invalid SampleSQL statement", None
    return None, {
        "recordEcl": scalar("EclRecord"),
        "partsJson": parts_json,
        "optionsJson": options_json,
        "sampleSql": sample_sql,
        "recordCount": record_count,
    }


def comparable_result(result):
    return {
        key: result.get(key)
        for key in ("columns", "rows", "rowCount", "returnedRowCount", "truncated")
    }


def run_file(endpoint, logical_name, limit, timeout):
    sql_name = logical_name.replace("'", "''")
    sql = f"SELECT * FROM hpcc_scan('{sql_name}') LIMIT {limit}"
    item = {"file": logical_name}
    query_started = time.monotonic()
    try:
        file_meta_error, file_meta = get_logical_file_meta(endpoint, logical_name, timeout)
        item["fileMetaSeconds"] = round(time.monotonic() - query_started, 4)
        if file_meta_error:
            item["fileMetaError"] = file_meta_error
        elif file_meta:
            item["recordCount"] = file_meta["recordCount"]

        normal_started = time.monotonic()
        normal_error, normal_result = execute_sql(endpoint, sql, timeout)
        item["seconds"] = round(time.monotonic() - normal_started, 4)
        if normal_error:
            unsupported_layout = f"unsupported record layout for '{logical_name}':"
            if (logical_name.rsplit("::", 1)[-1] == "dg_var"
                    and ((file_meta_error
                          and unsupported_layout in file_meta_error.lower()
                          and unsupported_layout in normal_error.lower())
                         or (not file_meta_error
                             and "hpcc_scan offline overload received an invalid ECL record definition" in normal_error))):
                item.update(status="expected_error", category="known_unsupported_layout", error=normal_error)
                return item
            item.update(status="error", category=classify(normal_error), error=normal_error)
            return item
        if normal_result is None:
            item.update(status="invalid_response", category="invalid_response")
            return item
        item.update(
            rows=normal_result.get("returnedRowCount"),
            columns=len(normal_result.get("columns", [])),
            truncated=normal_result.get("truncated"),
        )
        if file_meta_error or not file_meta:
            item.update(
                status="file_meta_error",
                category="file_meta_error",
                error=file_meta_error or "LogicalFileMetadata returned no serialized parts",
            )
            return item

        offline_sql = f"{file_meta['sampleSql']} LIMIT {limit}"
        offline_error, offline_result = execute_sql(endpoint, offline_sql, timeout)
        if offline_error and "offline overload is disabled" in offline_error:
            item.update(status="ok", offline="skipped (long-form scans are disabled in WsSQL2)")
        elif offline_error:
            item.update(status="offline_error", category=classify(offline_error), error=offline_error)
        elif offline_result is None:
            item.update(status="offline_invalid_response", category="invalid_response")
        elif comparable_result(normal_result) != comparable_result(offline_result):
            item.update(
                status="mismatch",
                category="file_meta_result_mismatch",
                normal=comparable_result(normal_result),
                offline=comparable_result(offline_result),
            )
        else:
            item["status"] = "ok"
    except Exception as exception:
        item["seconds"] = round(time.monotonic() - query_started, 4)
        item.update(status="transport_error", category="transport_error", error=repr(exception))
    return item


def main():
    parser = argparse.ArgumentParser(description="Run bounded hpcc_scan queries across a logical-file scope")
    parser.add_argument("--endpoint", default="http://127.0.0.1:8510/WsSQL2/")
    parser.add_argument("--dfu-endpoint", default="http://localhost:8010/WsDfu/DFUQuery.json")
    parser.add_argument("--scope", default="regress::*")
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--report", default="read-regress-errors.json",
                        help="JSON file written only when files fail; removed after a passing run")
    args = parser.parse_args()

    files = list_logical_files(args.dfu_endpoint, args.scope, args.timeout)
    started = time.monotonic()
    worker_count = max(1, args.workers)
    with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
        results = list(executor.map(
            lambda logical_name: run_file(args.endpoint, logical_name, args.limit, args.timeout),
            files,
        ))

    failures = [item for item in results if item["status"] not in ("ok", "expected_error")]
    summary = {
        "scope": args.scope,
        "limit": args.limit,
        "workers": worker_count,
        "elapsed": round(time.monotonic() - started, 3),
        "statusCounts": dict(collections.Counter(item["status"] for item in results)),
        "categoryCounts": dict(collections.Counter(
            item.get("category", "ok") for item in results)),
    }
    if failures:
        with open(args.report, "w", encoding="utf-8") as output:
            json.dump({**summary, "errors": failures}, output, indent=2)
        summary["report"] = args.report
    elif os.path.exists(args.report):
        os.remove(args.report)
    print(json.dumps(summary))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())