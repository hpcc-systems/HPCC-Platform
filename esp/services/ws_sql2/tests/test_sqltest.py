import importlib.machinery
import importlib.util
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from unittest.mock import patch

sql_test_loader = importlib.machinery.SourceFileLoader(
    "sql_test", os.path.join(os.path.dirname(__file__), "sql-test")
)
sql_test_spec = importlib.util.spec_from_loader(sql_test_loader.name, sql_test_loader)
sql_test = importlib.util.module_from_spec(sql_test_spec)
sys.modules[sql_test_loader.name] = sql_test
sql_test_loader.exec_module(sql_test)

from sql_test import (
    SqlClient,
    check_explain_expectations,
    compare_ecl_results,
    compare_timings,
    delete_ecl_workunit,
    discover_suite_queries,
    fetch_total_cluster_time,
    get_worker_count,
    load_config,
    main,
    parse_cluster_time,
    resolve_endpoint,
    resolve_queries,
    results_path,
    run_cursor_case,
    run_query,
    run_split_case,
    run_sql_ecl_case,
    run_suite,
    SUITE_OPERATIONS,
)


class FakeResponse:
    def __init__(self, body):
        self.body = body.encode()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def read(self):
        return self.body


class FakeClient:
    def __init__(self, result, execute_seconds=0.2):
        self.result = result
        self.execute_seconds = execute_seconds

    def split_query(self, sql):
        return [sql]

    def execute(self, sql):
        return self.result

    def execute_timed(self, sql):
        return self.result, self.execute_seconds


class SqlTestTests(unittest.TestCase):
    def test_parallelism_matches_ecl_test_semantics(self):
        self.assertEqual(get_worker_count(0), 1)
        self.assertEqual(get_worker_count(1), 1)
        self.assertEqual(get_worker_count(3), 3)
        self.assertEqual(get_worker_count(12), 12)
        with patch("sql_test.os.cpu_count", return_value=4):
            self.assertEqual(get_worker_count(-1), 8)
        with patch("sql_test.os.cpu_count", return_value=8):
            self.assertEqual(get_worker_count(-1), 16)

    def test_parallel_suite_runs_queries_concurrently(self):
        suite_dir = os.path.dirname(os.path.abspath(__file__))
        sql_dir = os.path.join(suite_dir, "sql")
        queries = resolve_queries(os.path.realpath(sql_dir), None)
        lock = threading.Lock()
        active = 0
        maximum_active = 0

        def fake_run_query(*args, **kwargs):
            nonlocal active, maximum_active
            with lock:
                active += 1
                maximum_active = max(maximum_active, active)
            time.sleep(0.05)
            with lock:
                active -= 1

        with patch("sql_test.run_query", side_effect=fake_run_query):
            with patch("sql_test.ThreadPoolExecutor", wraps=ThreadPoolExecutor) as executor:
                with patch("builtins.print"):
                    passed, failures = run_suite(
                        object(), queries, sql_dir, os.path.join(sql_dir, "ExecuteQuery"),
                        ignore_result=True, update_results=False, parallelism=12,
                    )

        executor.assert_called_once_with(max_workers=12)
        self.assertEqual(passed, len(queries))
        self.assertEqual(failures, [])
        self.assertGreaterEqual(maximum_active, 2)

    def test_parallel_suite_reports_completion_while_an_earlier_query_is_running(self):
        with tempfile.TemporaryDirectory() as sql_dir:
            slow_query = os.path.join(sql_dir, "slow.sql")
            quick_query = os.path.join(sql_dir, "quick.sql")
            slow_started = threading.Event()
            release_slow = threading.Event()
            quick_reported = threading.Event()
            lines = []
            result = []

            def fake_run_query(_client, query_path, *_args, **_kwargs):
                if query_path == slow_query:
                    slow_started.set()
                    release_slow.wait(5)
                    return None
                if not slow_started.wait(2):
                    raise RuntimeError("slow query did not start")
                return None

            def capture_print(message, **kwargs):
                lines.append((message, kwargs))
                if message.startswith("PASS quick.sql"):
                    quick_reported.set()

            with patch("sql_test.run_query", side_effect=fake_run_query):
                with patch("builtins.print", side_effect=capture_print):
                    runner = threading.Thread(
                        target=lambda: result.append(
                            run_suite(None, [slow_query, quick_query], sql_dir, sql_dir,
                                      False, False, 2)
                        )
                    )
                    runner.start()
                    try:
                        self.assertTrue(slow_started.wait(2), "slow query did not start")
                        self.assertTrue(
                            quick_reported.wait(2),
                            "quick query completion was hidden behind the earlier slow query",
                        )
                    finally:
                        release_slow.set()
                        runner.join(5)

            self.assertFalse(runner.is_alive(), "suite runner did not finish")
            self.assertEqual(result[0], (2, []))
            start_lines = [line for line, _ in lines if line.startswith("START ")]
            self.assertCountEqual(start_lines, ["START slow.sql", "START quick.sql"])
            self.assertTrue(all(kwargs.get("flush") for line, kwargs in lines if line.startswith("START ")))
            self.assertTrue(any(
                re.match(r"PASS quick\.sql\s+\(\d+\.\d+s\)", line)
                for line, _ in lines
            ))

    def test_parallel_suite_reports_failures_without_stopping(self):
        suite_dir = os.path.dirname(os.path.abspath(__file__))
        sql_dir = os.path.join(suite_dir, "sql")
        queries = resolve_queries(os.path.realpath(sql_dir), None)

        def fail_one(_client, query_path, *args, **kwargs):
            if query_path.endswith("range_page.sql"):
                raise RuntimeError("expected failure")

        with patch("sql_test.run_query", side_effect=fail_one):
            with patch("builtins.print"):
                passed, failures = run_suite(
                    object(), queries, sql_dir, os.path.join(sql_dir, "ExecuteQuery"),
                    ignore_result=True, update_results=False, parallelism=2,
                )

        self.assertEqual(passed, len(queries) - 1)
        self.assertEqual([name for name, _ in failures], ["range_page.sql"])

    def test_run_subcommand_runs_the_default_suite(self):
        suite_dir = os.path.dirname(os.path.abspath(__file__))
        expected_config = os.path.join(suite_dir, "sql-test.json")
        self.assertTrue(os.path.isfile(expected_config))
        result = {"columns": ["value"], "rows": [[1]]}
        profile = {
            "latency": 0.01,
            "rows_returned": 1,
            "children": [{"name": "HPCC_SCAN", "operator_cardinality": 1, "operator_timing": 0.01}],
        }

        def fake_execute(_client, sql, include_metrics=False):
            if "dg_var" in sql:
                raise RuntimeError(
                    "Binder Error: Unsupported record layout for 'regress::multi::dg_var': "
                    "runtime metadata is unavailable"
                )
            return (result, profile) if include_metrics else result

        with patch("sql_test.SqlClient.execute", autospec=True, side_effect=fake_execute) as execute:
            with patch("sql_test.SqlClient.explain_query", return_value={"text": "", "json": {}}):
                with patch("sql_test.SqlClient.split_query", autospec=True,
                           side_effect=lambda _client, sql: [sql]):
                    with patch("builtins.print"):
                        result_code = main(["run", "--ignoreResult"])

        self.assertEqual(result_code, 0)
        self.assertEqual(
            execute.call_count,
            sum(mode not in ("sql-explain", "sql-split")
                for mode, _, _ in discover_suite_queries(suite_dir, [])),
        )

    @patch("sql_test.SqlClient.split_query", autospec=True, side_effect=lambda _client, sql: [sql])
    def test_suite_option_runs_only_paired_ecl_cases(self, _split):
        with patch("sql_test.SqlClient.execute_timed",
                   return_value=({"columns": ["n"], "rows": [[0], [1], [2]]}, 0.004)):
            with patch("sql_test.subprocess.run") as ecl_run:
                ecl_run.return_value.returncode = 0
                ecl_run.return_value.stdout = (
                    "   wuid: W20261018-101141\n"
                    "<Dataset name='Result'><Row><n>0</n></Row>"
                    "<Row><n>1</n></Row><Row><n>2</n></Row></Dataset>"
                )
                with patch("sql_test.fetch_total_cluster_time", return_value="0.011"):
                    with patch("sql_test.delete_ecl_workunit") as delete:
                        with patch("builtins.print") as print_result:
                            result = main(["--suite", "sql-ecl", "range_rows.sql"])

        self.assertEqual(result, 0)
        ecl_run.assert_called_once()
        delete.assert_called_once()
        self.assertTrue(any(
            re.fullmatch(
                r"PASS sql-ecl/range_rows\.sql  SQL +0\.004s < "
                r"ECL +0\.011  \(end-to-end SQL +\d+\.\d{3}s [<>=] ECL +\d+\.\d{3}s\)"
                r"  \(\d+\.\d+s total\)",
                call.args[0],
            )
            for call in print_result.call_args_list
        ))

    def test_ecl_timings_align_across_names(self):
        timings = {"a.sql": (0.5, "1.726", 0.6, 3.6), "much_longer.sql": (12.25, "10.5", 15.5, 14.0)}

        def fake_run_query(_client, path, *_args):
            return timings[os.path.basename(path)]

        with tempfile.TemporaryDirectory() as sql_dir:
            queries = [os.path.join(sql_dir, name) for name in timings]
            with patch("sql_test.run_query", side_effect=fake_run_query):
                with patch("builtins.print") as print_result:
                    run_suite(None, queries, sql_dir, sql_dir, False, False, 0, "sql-ecl", {})

        lines = [call.args[0] for call in print_result.call_args_list if call.args[0].startswith("PASS ")]
        self.assertEqual(len(lines), 2)
        for label in ("SQL", "ECL", "(end-to-end SQL", " ECL", "s)"):
            self.assertEqual(len({line.rindex(label) for line in lines}), 1, lines)
        self.assertEqual([line[line.index("ECL") - 2] for line in lines], ["<", ">"], lines)
        self.assertEqual([line[line.rindex("ECL") - 2] for line in lines], ["<", ">"], lines)

    def test_parse_cluster_time_formats(self):
        self.assertEqual(parse_cluster_time("0.011"), Decimal("0.011"))
        self.assertEqual(parse_cluster_time("1:02.345"), Decimal("62.345"))
        self.assertEqual(parse_cluster_time("1:02:03.456"), Decimal("3723.456"))
        self.assertEqual(parse_cluster_time("2 days 0:0:05.000"), Decimal("172805.000"))
        self.assertIsNone(parse_cluster_time("n/a"))

    def test_compare_timings_shows_faster_side(self):
        self.assertEqual(compare_timings(0.5, "1.726"), "<")
        self.assertEqual(compare_timings(70.0, "1:02.345"), ">")
        self.assertEqual(compare_timings(0.0114, "0.011"), "=")
        self.assertEqual(compare_timings(0.5, "n/a"), "?")

    def test_discovery_includes_each_mode_suite(self):
        with tempfile.TemporaryDirectory() as root:
            expected = []
            for mode in ("sql", "sql-cursor", "sql-ecl", "sql-equivalence", "sql-split", "sql-xfail"):
                mode_dir = os.path.join(root, mode)
                os.makedirs(mode_dir)
                query = os.path.join(mode_dir, f"{mode}.sql")
                with open(query, "w", encoding="utf-8") as file:
                    file.write("SELECT 1")
                if mode == "sql-ecl":
                    with open(os.path.splitext(query)[0] + ".ecl", "w", encoding="utf-8") as file:
                        file.write("OUTPUT(1, NAMED('Result'));")
                expected.append((mode, os.path.realpath(mode_dir), os.path.realpath(query)))

            self.assertEqual(discover_suite_queries(root, None), sorted(expected))
            os.remove(os.path.join(root, "sql-ecl", "sql-ecl.ecl"))
            with self.assertRaisesRegex(ValueError, "Missing ECL test file"):
                discover_suite_queries(root, None)

    def test_expected_failure_suite_matches_error_text(self):
        with tempfile.TemporaryDirectory() as suite:
            sql_dir = os.path.join(suite, "sql-xfail")
            results_dir = os.path.join(sql_dir, "ExecuteQuery")
            os.makedirs(results_dir)
            query_path = os.path.join(sql_dir, "bad_layout.sql")
            with open(query_path, "w", encoding="utf-8") as file:
                file.write("SELECT invalid")
            with open(os.path.join(results_dir, "bad_layout.json"), "w", encoding="utf-8") as file:
                json.dump({"errorContains": "unsupported layout"}, file)

            class FailingClient:
                def execute(self, _sql):
                    raise RuntimeError("unsupported layout from service")

            run_query(
                FailingClient(), query_path, sql_dir, results_dir,
                ignore_result=False, update_results=False, mode="sql-xfail",
            )

    def test_cursor_suite_checks_exact_batches_and_cursor_states(self):
        class CursorClient:
            def __init__(self):
                self.cursor_id = "cursor-1"
                self.fetched = []
                self.closed = []

            def execute_first_batch(self, _sql, _fetch_size):
                return {"rows": [[0], [1]], "cursor_id": self.cursor_id}

            def fetch_batch(self, cursor_id, _sql, _fetch_size):
                self.fetched.append(cursor_id)
                if len(self.fetched) == 1:
                    return {"rows": [[2], [3]], "cursor_id": cursor_id}
                return {"rows": [], "cursor_id": ""}

            def close_cursor(self, cursor_id, _sql):
                self.closed.append(cursor_id)
                return True

        client = CursorClient()
        run_cursor_case(client, "SELECT 1", {
            "fetchSize": 2,
            "firstRows": [[0], [1]],
            "cursorAfterFirst": True,
            "followingBatches": [[[2], [3]], []],
            "cursorAfterFollowingBatches": [True, False],
            "sameCursorAfterFollowingBatches": [True, False],
        })
        self.assertEqual(client.fetched, ["cursor-1", "cursor-1"])
        self.assertEqual(client.closed, [])

    def test_cursor_suite_cancels_a_running_first_fetch(self):
        class CancelClient:
            def __init__(self, stops_on_close=True):
                self.stops_on_close = stops_on_close
                self.closed = threading.Event()

            def open_cursor(self, _sql):
                return "cursor-1"

            def fetch_batch(self, _cursor_id, _sql, _fetch_size):
                self.closed.wait(5)
                if self.stops_on_close:
                    raise RuntimeError("FetchResultSet failed for SELECT 1: 20001 Query was cancelled")
                return {"rows": [[1]], "cursor_id": ""}

            def close_cursor(self, _cursor_id, _sql):
                self.closed.set()
                return True

        expected = {
            "fetchSize": 1,
            "scenario": "cancel-during-fetch",
            "cancelAfterSeconds": 0.01,
            "stopWithinSeconds": 5,
            "fetchErrorContains": ["20001", "Query was cancelled"],
        }
        run_cursor_case(CancelClient(), "SELECT 1", expected)
        with self.assertRaisesRegex(AssertionError, "completed although its cursor was cancelled"):
            run_cursor_case(CancelClient(stops_on_close=False), "SELECT 1", expected)

    def test_first_batch_opens_then_fetches_and_reports_completion(self):
        execute_response = '<Response><CursorId>cursor-1</CursorId></Response>'
        fetch_response = '<Response><RowsJSON>[[1]]</RowsJSON><Complete>true</Complete></Response>'
        with patch("sql_test.urllib.request.urlopen",
                   side_effect=[FakeResponse(execute_response), FakeResponse(fetch_response)]) as urlopen:
            batch = SqlClient("http://localhost/WsSQL2/", 10).execute_first_batch("SELECT 1", 2)

        self.assertEqual(batch, {"rows": [[1]], "cursor_id": ""})
        fetch_request = urlopen.call_args_list[1].args[0]
        self.assertIn(b"<RowCount>2</RowCount>", fetch_request.data)
        self.assertNotIn(b"<RowCount>", urlopen.call_args_list[0].args[0].data)

    def test_first_batch_reports_cursor_close_failure(self):
        client = SqlClient("http://localhost/WsSQL2/", 10)
        with (patch.object(client, "open_cursor", return_value="cursor-1"),
              patch.object(client, "fetch_batch", side_effect=RuntimeError("fetch failed")),
              patch.object(client, "close_cursor", side_effect=RuntimeError("close failed"))):
            with self.assertRaisesRegex(
                RuntimeError,
                "fetch failed; failed to close cursor cursor-1: close failed",
            ):
                client.execute_first_batch("SELECT 1", 2)

    def test_split_query_returns_statements_in_order(self):
        response = (
            '<Response><Statements><Statement>SELECT 1</Statement>'
            '<Statement>SELECT \'a;b\'</Statement></Statements></Response>'
        )
        with patch("sql_test.urllib.request.urlopen", return_value=FakeResponse(response)) as urlopen:
            statements = SqlClient("http://localhost/WsSQL2/", 10).split_query("SELECT 1; SELECT 'a;b'")

        self.assertEqual(statements, ["SELECT 1", "SELECT 'a;b'"])
        request = urlopen.call_args.args[0]
        self.assertIn(b"<SplitQueryRequest", request.data)
        self.assertEqual(request.headers["Soapaction"], '"SplitQuery"')
        with patch("sql_test.urllib.request.urlopen", return_value=FakeResponse("<Response/>")):
            self.assertEqual(SqlClient("http://localhost/WsSQL2/", 10).split_query(" "), [])

    def test_split_case_checks_statements_and_errors(self):
        class SplitClient:
            def __init__(self, statements=None, error=None):
                self.statements = statements
                self.error = error

            def split_query(self, _sql):
                if self.error:
                    raise RuntimeError(self.error)
                return self.statements

        run_split_case(SplitClient(["SELECT 1"]), "SELECT 1", {"statements": ["SELECT 1"]}, "case.sql")
        with self.assertRaisesRegex(AssertionError, "Split statements differ"):
            run_split_case(SplitClient(["SELECT 2"]), "SELECT 2", {"statements": ["SELECT 1"]}, "case.sql")
        run_split_case(SplitClient(error="syntax error at or near"), "SELEC", {"errorContains": "syntax error"},
                       "case.sql")
        with self.assertRaisesRegex(AssertionError, "Expected split to fail"):
            run_split_case(SplitClient([]), "SELECT 1", {"errorContains": "syntax error"}, "case.sql")
        with self.assertRaisesRegex(AssertionError, "Unexpected error"):
            run_split_case(SplitClient(error="denied"), "SELECT 1", {"statements": []}, "case.sql")
        with self.assertRaisesRegex(ValueError, "requires statements or errorContains"):
            run_split_case(SplitClient([]), "SELECT 1", {}, "case.sql")
        run_split_case(SplitClient(error="anything"), "SELEC", None, "case.sql")

    def test_explain_suite_checks_text_and_plan_details(self):
        check_explain_expectations({
            "text": "Pushed Limit: 20",
            "json": {"name": "HPCC_SCAN", "extra_info": {"Keyed Filters": ["kind=1", "word='a'"]}},
        }, {
            "contains": ["Pushed Limit: 20"],
            "node": "HPCC_SCAN",
            "extraInfoContains": {"Keyed Filters": ["kind=", "word="]},
        }, "SELECT 1")

    def test_explain_checks_parts_from_logical_file_metadata(self):
        expected = {
            "node": "HPCC_SCAN",
            "scanPartsFromLogicalFile": "regress::multi::searchindex",
            "limitByPartCount": {"limit": 20},
        }

        def check(parts, scan_info, limit_node):
            meta_response = (
                "<Response><LogicalFileMetadata><Kind>key</Kind><FileParts>"
                + json.dumps({"part": [{"@tlk": flag} for flag in parts]})
                + "</FileParts></LogicalFileMetadata></Response>"
            )
            plan = {"text": "", "json": {"name": "HPCC_SCAN", "extra_info": scan_info}}
            if limit_node:
                plan["json"] = {"name": "STREAMING_LIMIT", "children": [plan["json"]]}
            with patch("sql_test.urllib.request.urlopen", return_value=FakeResponse(meta_response)) as urlopen:
                check_explain_expectations(
                    plan, expected, "SELECT * FROM hpcc_scan('regress::multi::searchindex') LIMIT 20",
                    SqlClient("http://localhost/WsSQL2/", 10),
                )
            self.assertIn(b"<LogicalName>regress::multi::searchindex</LogicalName>",
                          urlopen.call_args.args[0].data)

        check(["0", "0", "1"], {
            "Physical Parts": "3", "Key Data Parts": "2", "Max Threads": "2",
            "Partitioned Scan": "true", "Limit Pushdown": "false",
        }, True)
        check(["0", "1"], {
            "Physical Parts": "2", "Key Data Parts": "1", "Max Threads": "1",
            "Partitioned Scan": "false", "Limit Pushdown": "true", "Pushed Limit": "20",
        }, False)
        with self.assertRaisesRegex(AssertionError, "Max Threads"):
            check(["0", "0", "1"], {
                "Physical Parts": "3", "Key Data Parts": "2", "Max Threads": "3",
                "Partitioned Scan": "true", "Limit Pushdown": "false",
            }, True)

    def test_metrics_suite_checks_results_and_profile(self):
        with tempfile.TemporaryDirectory() as suite:
            sql_dir = os.path.join(suite, "sql-metrics")
            results_dir = os.path.join(sql_dir, "ExecuteQuery")
            os.makedirs(results_dir)
            query_path = os.path.join(sql_dir, "profile.sql")
            with open(query_path, "w", encoding="utf-8") as file:
                file.write("SELECT 1")
            expected = {
                "result": {"columns": ["value"], "rows": [[1]]},
                "metrics": {
                    "node": "HPCC_SCAN",
                    "requiredNodeFields": ["operator_cardinality", "operator_timing"],
                    "requiredProfileFields": ["latency", "rows_returned"],
                    "rows_returned": 1,
                },
            }
            with open(os.path.join(results_dir, "profile.json"), "w", encoding="utf-8") as file:
                json.dump(expected, file)

            class MetricsClient:
                def execute(self, _sql, include_metrics=False):
                    self.include_metrics = include_metrics
                    return (
                        expected["result"],
                        {
                            "latency": 0.01,
                            "rows_returned": 1,
                            "children": [{
                                "name": "HPCC_SCAN",
                                "operator_cardinality": 1,
                                "operator_timing": 0.01,
                            }],
                        },
                    )

            client = MetricsClient()
            run_query(
                client, query_path, sql_dir, results_dir,
                ignore_result=False, update_results=False, mode="sql-metrics",
            )
            self.assertTrue(client.include_metrics)

    def test_equivalence_suite_compares_results_from_both_queries(self):
        with tempfile.TemporaryDirectory() as suite:
            sql_dir = os.path.join(suite, "sql-equivalence")
            results_dir = os.path.join(sql_dir, "Compare")
            os.makedirs(results_dir)
            query_path = os.path.join(sql_dir, "comparison.sql")
            with open(query_path, "w", encoding="utf-8") as file:
                file.write("SELECT 1")
            with open(os.path.join(results_dir, "comparison.json"), "w", encoding="utf-8") as file:
                json.dump({"equivalentTo": "SELECT 1 AS other"}, file)

            run_query(
                FakeClient({"columns": ["value"], "rows": [[1]]}),
                query_path, sql_dir, results_dir,
                ignore_result=False, update_results=False, mode="sql-equivalence",
            )

            class DifferentClient:
                def __init__(self):
                    self.calls = 0

                def execute(self, _sql):
                    self.calls += 1
                    value = self.calls
                    return {"columns": ["value"], "rows": [[value]]}

            with self.assertRaisesRegex(AssertionError, "Results differ"):
                run_query(
                    DifferentClient(),
                    query_path, sql_dir, results_dir,
                    ignore_result=False, update_results=False, mode="sql-equivalence",
                )

    def test_sql_ecl_compares_typed_rows_and_reports_both_timings(self):
        with tempfile.TemporaryDirectory() as suite:
            query_path = os.path.join(suite, "case.sql")
            ecl_path = os.path.join(suite, "case.ecl")
            with open(query_path, "w", encoding="utf-8") as file:
                file.write("SELECT 1 AS n")
            with open(ecl_path, "w", encoding="utf-8") as file:
                file.write("OUTPUT(DATASET(...), NAMED('Result'));")
            config = {
                "target": "hthor", "cluster": "hthor", "server": "localhost",
                "port": "8010", "ssl": False, "username": "", "password": "", "timeout": 30,
            }
            result = {"columns": ["n"], "rows": [[1]]}
            completed = type("Result", (), {
                "stdout": "   wuid: W20261018-101141\n"
                          "<Dataset name='Result'><Row><n>1.0</n></Row></Dataset>",
                "stderr": "", "returncode": 0,
            })()
            with patch("sql_test.subprocess.run", return_value=completed) as run:
                with patch("sql_test.fetch_total_cluster_time", return_value="0.011") as fetch:
                    with patch("sql_test.delete_ecl_workunit") as delete:
                        with patch("sql_test.time.monotonic", side_effect=[1, 1.5, 2, 3]):
                            timings = run_sql_ecl_case(FakeClient(result), "SELECT 1 AS n", ecl_path, config)
            self.assertAlmostEqual(timings[0], 0.2)
            self.assertEqual(timings[1:], ("0.011", 0.5, 1))
            fetch.assert_called_once_with("W20261018-101141", config)
            delete.assert_called_once_with("W20261018-101141", config)
            self.assertEqual(run.call_args.args[0][-1], ecl_path)
            self.assertIn("--target=hthor", run.call_args.args[0])
            self.assertIn("-v", run.call_args.args[0])
            self.assertIn("--noroot", run.call_args.args[0])
            self.assertEqual(run.call_args.kwargs["timeout"], 90)

            with patch("sql_test.subprocess.run", return_value=completed):
                with patch("sql_test.fetch_total_cluster_time", return_value="0.011"):
                    with patch("sql_test.delete_ecl_workunit"):
                        with patch("sql_test.time.monotonic", side_effect=[1, 1.5, 2, 3]):
                            timings = run_query(
                                FakeClient(result), query_path, suite, os.path.join(suite, "ECL"),
                                ignore_result=False, update_results=False, mode="sql-ecl", ecl_config=config,
                            )
            self.assertEqual(timings[1:], ("0.011", 0.5, 1))
            os.remove(ecl_path)
            with self.assertRaisesRegex(RuntimeError, "Missing ECL test file"):
                run_sql_ecl_case(FakeClient(result), "SELECT 1", ecl_path, config)

    def test_sql_ecl_deletes_workunit_after_failure(self):
        config = {
            "target": "hthor", "cluster": "hthor", "server": "localhost",
            "port": "8010", "ssl": False, "username": "", "password": "", "timeout": 30,
        }
        with tempfile.TemporaryDirectory() as suite:
            ecl_path = os.path.join(suite, "case.ecl")
            with open(ecl_path, "w", encoding="utf-8") as file:
                file.write("OUTPUT(DATASET(...), NAMED('Result'));")
            for stdout, returncode, timing_error in (
                ("wuid: W20261018-101141\n<Dataset name='Result'><Row><n>2</n></Row></Dataset>", 0, None),
                ("wuid: W20261018-101141\n", 1, None),
                ("wuid: W20261018-101141\n<Dataset name='Result'><Row><n>1</n></Row></Dataset>",
                 0, RuntimeError("timing unavailable")),
            ):
                completed = type("Result", (), {"stdout": stdout, "stderr": "", "returncode": returncode})()
                with patch("sql_test.subprocess.run", return_value=completed):
                    with patch("sql_test.fetch_total_cluster_time", side_effect=timing_error):
                        with patch("sql_test.delete_ecl_workunit") as delete:
                            with self.assertRaises((AssertionError, RuntimeError)):
                                run_sql_ecl_case(FakeClient({"columns": ["n"], "rows": [[1]]}),
                                                 "SELECT 1 AS n", ecl_path, config)
                delete.assert_called_once_with("W20261018-101141", config)
            with patch("sql_test.subprocess.run", side_effect=subprocess.TimeoutExpired(
                "ecl run", 90, output=b"wuid: W20261018-101141\n"
            )):
                with patch("sql_test.delete_ecl_workunit") as delete:
                    with self.assertRaisesRegex(RuntimeError, "ECL timed out"):
                        run_sql_ecl_case(FakeClient({"columns": ["n"], "rows": [[1]]}),
                                         "SELECT 1 AS n", ecl_path, config)
                delete.assert_called_once_with("W20261018-101141", config)
            completed = type("Result", (), {
                "stdout": "wuid: W20261018-101141\n"
                          "<Dataset name='Result'><Row><n>2</n></Row></Dataset>",
                "stderr": "", "returncode": 0,
            })()
            with patch("sql_test.subprocess.run", return_value=completed):
                with patch("sql_test.delete_ecl_workunit", side_effect=RuntimeError("delete denied")):
                    with self.assertRaisesRegex(RuntimeError, "Row 1, column n:.*; delete denied"):
                        run_sql_ecl_case(FakeClient({"columns": ["n"], "rows": [[1]]}),
                                         "SELECT 1 AS n", ecl_path, config)

    def test_sql_ecl_compares_each_statement_with_its_ecl_dataset(self):
        class MultiClient:
            def __init__(self):
                self.executed = []

            def split_query(self, _sql):
                return ["SELECT 1 AS n", "SELECT broken", "SELECT 'a' AS s"]

            def execute_timed(self, sql):
                self.executed.append(sql)
                if sql == "SELECT broken":
                    raise RuntimeError("Binder Error: broken")
                if sql == "SELECT 1 AS n":
                    return {"columns": ["n"], "rows": [[1]]}, 0.25
                return {"columns": ["s"], "rows": [["a"]]}, 0.5

        config = {
            "target": "hthor", "cluster": "hthor", "server": "localhost",
            "port": "8010", "ssl": False, "username": "", "password": "", "timeout": 30,
        }
        datasets = (
            "<Dataset name='Result 1'><Row><n>1</n></Row></Dataset>"
            "<Dataset name='Result 2'><Row><n>2</n></Row></Dataset>"
            "<Dataset name='Result 3'><Row><s>a</s></Row></Dataset>"
        )
        with tempfile.TemporaryDirectory() as suite:
            ecl_path = os.path.join(suite, "case.ecl")
            with open(ecl_path, "w", encoding="utf-8") as file:
                file.write("OUTPUT(1); OUTPUT(2); OUTPUT('a');")
            completed = type("Result", (), {
                "stdout": "wuid: W20261018-101141\n" + datasets, "stderr": "", "returncode": 0,
            })()
            client = MultiClient()
            with patch("sql_test.subprocess.run", return_value=completed):
                with patch("sql_test.delete_ecl_workunit") as delete:
                    with self.assertRaisesRegex(
                        AssertionError, r"Result 2 \(ECL 'Result 2'\): SQL failed: Binder Error: broken"
                    ):
                        run_sql_ecl_case(client, "script", ecl_path, config)
            self.assertEqual(client.executed, MultiClient().split_query(""))
            delete.assert_called_once_with("W20261018-101141", config)

            client.split_query = lambda _sql: ["SELECT 1 AS n", "SELECT 'a' AS s"]
            completed.stdout = (
                "wuid: W20261018-101141\n"
                "<Dataset name='Result 1'><Row><n>1</n></Row></Dataset>"
                "<Dataset name='Result 2'><Row><s>a</s></Row></Dataset>"
            )
            with patch("sql_test.subprocess.run", return_value=completed):
                with patch("sql_test.fetch_total_cluster_time", return_value="0.011"):
                    with patch("sql_test.delete_ecl_workunit"):
                        timings = run_sql_ecl_case(client, "script", ecl_path, config)
            self.assertAlmostEqual(timings[0], 0.75)

    def test_compare_ecl_results_reports_every_mismatch(self):
        sql_results = [
            {"columns": ["n"], "rows": [[1]]},
            {"columns": ["n"], "rows": [[3]]},
            RuntimeError("unsupported"),
        ]
        ecl_output = (
            "<Dataset name='Result 1'><Row><n>1</n></Row></Dataset>"
            "<Dataset name='Result 2'><Row><n>2</n></Row></Dataset>"
        )
        with self.assertRaises(AssertionError) as raised:
            compare_ecl_results(sql_results, ecl_output)
        message = str(raised.exception)
        self.assertIn("SQL returned 3 result sets but ECL returned 2", message)
        self.assertIn("Result 2 (ECL 'Result 2'): Row 1, column n: SQL 3 != ECL '2'", message)
        self.assertIn("Result 3 (no ECL Dataset): SQL failed: unsupported", message)
        self.assertNotIn("Result 1", message)
        with self.assertRaisesRegex(RuntimeError, "only named Datasets"):
            compare_ecl_results(sql_results, "<Row/>")

    def test_delete_ecl_workunit_requires_success(self):
        config = {
            "server": "localhost", "port": "8010", "ssl": False,
            "username": "", "password": "", "timeout": 10,
        }
        with patch("sql_test.urllib.request.urlopen",
                   return_value=FakeResponse('{"WUDeleteResponse":{}}')) as urlopen:
            delete_ecl_workunit("W20261018-101141", config)
        request = urlopen.call_args.args[0]
        self.assertEqual(request.get_method(), "POST")
        self.assertIn("/WsWorkunits/WUDelete.json?Wuids=W20261018-101141", request.full_url)
        with patch("sql_test.urllib.request.urlopen",
                   return_value=FakeResponse(
                       '{"WUDeleteResponse":{"ActionResults":{"WUActionResult":'
                       '[{"Wuid":"W20261018-101141","Result":"Failed: protected"}]}}}'
                   )):
            with self.assertRaisesRegex(RuntimeError, "Failed: protected"):
                delete_ecl_workunit("W20261018-101141", config)
        with patch("sql_test.urllib.request.urlopen",
                   return_value=FakeResponse('{"WUDeleteResponse":{"Exceptions":{"Exception":[{"Message":"denied"}]}}}')):
            with self.assertRaisesRegex(RuntimeError, "denied"):
                delete_ecl_workunit("W20261018-101141", config)

    def test_total_cluster_time_requires_workunit_and_reported_value(self):
        config = {
            "server": "localhost", "port": "8010", "ssl": False,
            "username": "", "password": "", "timeout": 10,
        }
        response = ('{"WUQueryResponse":{"Workunits":{"ECLWorkunit":'
                    '[{"Wuid":"W20261018-101141","TotalClusterTime":"1:02.345"}]}}}')
        with patch("sql_test.urllib.request.urlopen", return_value=FakeResponse(response)) as urlopen:
            self.assertEqual(fetch_total_cluster_time("W20261018-101141", config), "1:02.345")
        self.assertIn("/WsWorkunits/WUQuery.json?", urlopen.call_args.args[0].full_url)
        self.assertIn("Wuid=W20261018-101141%2A", urlopen.call_args.args[0].full_url)
        self.assertIn("PageSize=2", urlopen.call_args.args[0].full_url)
        self.assertIn("Sortby=Wuid", urlopen.call_args.args[0].full_url)
        with patch("sql_test.urllib.request.urlopen",
                   return_value=FakeResponse(
                       '{"WUQueryResponse":{"Workunits":{"ECLWorkunit":[{"Wuid":"W20261018-101141"}]}}}'
                   )):
            with self.assertRaisesRegex(RuntimeError, "did not return TotalClusterTime"):
                fetch_total_cluster_time("W20261018-101141", config)
        with patch("sql_test.urllib.request.urlopen",
                   return_value=FakeResponse(
                       '{"WUQueryResponse":{"Workunits":{"ECLWorkunit":'
                       '[{"Wuid":"W20261018-101141-1","TotalClusterTime":"2.000"}]}}}'
                   )):
            with self.assertRaisesRegex(RuntimeError, "did not return TotalClusterTime"):
                fetch_total_cluster_time("W20261018-101141", config)

    def test_sql_ecl_rejects_mismatch_and_unexpected_datasets(self):
        with self.assertRaisesRegex(AssertionError, "Row 1, column n"):
            compare_ecl_results(
                [{"columns": ["n"], "rows": [[1]]}],
                "<Dataset name='Result'><Row><n>2</n></Row></Dataset>",
            )
        with self.assertRaisesRegex(RuntimeError, "named Dataset"):
            compare_ecl_results([{"columns": ["n"], "rows": []}], "")
        with self.assertRaisesRegex(RuntimeError, "named Dataset"):
            compare_ecl_results([{"columns": ["n"], "rows": [[1]]}], "<Dataset><Row><n>1</n></Row></Dataset>")
        with self.assertRaisesRegex(AssertionError, "columns"):
            compare_ecl_results(
                [{"columns": ["n"], "rows": [[1]]}],
                "<Dataset name='Result'><Row><value>1</value></Row></Dataset>",
            )
        compare_ecl_results(
            [{"columns": ["Flag", "text", "missing"], "rows": [[True, "", None]]}],
            "<Dataset name='Result'><Row><flag>true</flag><text/>"
            "<missing xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:nil='true'/>"
            "</Row></Dataset>",
        )

    def test_default_sql_test_config_has_required_connection_settings(self):
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql-test.json")
        config = load_config(config_path)
        self.assertEqual(
            set(config),
            {"espIp", "espSocket", "eclTarget", "wsSql2Port", "useSsl", "username", "password", "timeout"},
        )
        self.assertEqual(resolve_endpoint(config), "http://127.0.0.1:8510/WsSQL2/")

    def test_bundled_sql_tests_have_execute_query_results(self):
        suite_dir = os.path.dirname(os.path.abspath(__file__))
        sql_dir = os.path.join(suite_dir, "sql")
        results_dir = os.path.join(sql_dir, "ExecuteQuery")
        queries = resolve_queries(os.path.realpath(sql_dir), None)

        self.assertTrue(queries)
        for query_path in queries:
            self.assertTrue(os.path.getsize(query_path), query_path)
            with open(results_path(query_path, os.path.realpath(sql_dir), results_dir),
                      encoding="utf-8") as results_file:
                expected_results = json.load(results_file)
            self.assertEqual(set(expected_results), {"columns", "rows"}, query_path)

    def test_bundled_specialized_suites_have_expected_assertions(self):
        suite_dir = os.path.dirname(os.path.abspath(__file__))
        cases = discover_suite_queries(suite_dir, None)
        modes = set()
        for mode, sql_dir, query_path in cases:
            if mode == "sql":
                continue
            modes.add(mode)
            if mode == "sql-ecl":
                self.assertTrue(os.path.isfile(os.path.splitext(query_path)[0] + ".ecl"))
                continue
            results_dir = os.path.join(sql_dir, SUITE_OPERATIONS[mode])
            expected_path = results_path(query_path, sql_dir, results_dir)
            self.assertTrue(os.path.isfile(expected_path), expected_path)
            with open(expected_path, encoding="utf-8") as expected_file:
                expected = json.load(expected_file)
            if mode == "sql-xfail":
                self.assertTrue(expected.get("errorContains"), expected_path)
            elif mode == "sql-cursor":
                self.assertGreater(expected.get("fetchSize", 0), 0, expected_path)
            elif mode == "sql-equivalence":
                self.assertTrue(
                    expected.get("equivalentTo") or "rowCount" in expected,
                    expected_path,
                )
            elif mode == "sql-metrics":
                self.assertTrue(expected.get("metrics", {}).get("requiredNodeFields"), expected_path)
            elif mode == "sql-explain":
                self.assertTrue(expected.get("node") or expected.get("contains"), expected_path)
            elif mode == "sql-split":
                self.assertTrue(
                    isinstance(expected.get("statements"), list) or expected.get("errorContains"),
                    expected_path,
                )

        self.assertEqual(modes, set(SUITE_OPERATIONS) - {"sql"})

    def test_query_discovers_sql_files_and_ignores_other_files(self):
        with tempfile.TemporaryDirectory() as suite:
            sql_dir = os.path.join(suite, "sql")
            os.makedirs(os.path.join(sql_dir, "nested"))
            root_sql = os.path.join(sql_dir, "root.sql")
            nested_sql = os.path.join(sql_dir, "nested", "case.sql")
            with open(root_sql, "w", encoding="utf-8") as file:
                file.write("SELECT 1")
            with open(nested_sql, "w", encoding="utf-8") as file:
                file.write("SELECT 2")
            with open(os.path.join(sql_dir, "notes.txt"), "w", encoding="utf-8") as file:
                file.write("not a test")

            self.assertEqual(
                resolve_queries(os.path.realpath(sql_dir), ["**/*.sql"]),
                sorted((os.path.realpath(root_sql), os.path.realpath(nested_sql))),
            )
            self.assertEqual(
                results_path(os.path.realpath(nested_sql), os.path.realpath(sql_dir),
                         os.path.join(suite, "sql", "ExecuteQuery")),
                os.path.join(suite, "sql", "ExecuteQuery", "nested", "case.json"),
            )

    def test_endpoint_defaults_to_regression_esp_settings(self):
        with patch.dict("os.environ", {}, clear=True):
            self.assertEqual(
                resolve_endpoint({"espIp": "localhost", "espSocket": "8010", "useSsl": "True"}),
                "https://localhost:8510/WsSQL2/",
            )

    def test_server_host_uses_configured_port(self):
        self.assertEqual(
            resolve_endpoint({"espSocket": "8010"}, "localhost"),
            "http://localhost:8510/WsSQL2/",
        )
        self.assertEqual(
            resolve_endpoint({"wsSql2Port": "8010"}, "localhost"),
            "http://localhost:8010/WsSQL2/",
        )
        self.assertEqual(
            resolve_endpoint({"wsSql2Port": "8510"}, "localhost:8511"),
            "http://localhost:8511/WsSQL2/",
        )

    def test_execute_collects_rows_across_cursor_fetches(self):
        execute_response = (
            '<Response><ResultSetMetadata><Column><ColumnLabel>n</ColumnLabel></Column></ResultSetMetadata>'
            '<CursorId>cursor-1</CursorId></Response>'
        )
        first_fetch = '<Response><RowsJSON>[[1]]</RowsJSON><Complete>false</Complete></Response>'
        last_fetch = '<Response><RowsJSON>[[2]]</RowsJSON><Complete>true</Complete></Response>'
        with patch("sql_test.urllib.request.urlopen",
                   side_effect=[FakeResponse(execute_response), FakeResponse(first_fetch),
                                FakeResponse(last_fetch)]) as urlopen:
            result = SqlClient("http://localhost/WsSQL2/", 10).execute("SELECT n")

        self.assertEqual(result, {"columns": ["n"], "rows": [[1], [2]]})
        self.assertEqual(urlopen.call_count, 3)
        self.assertEqual(urlopen.call_args_list[1].args[0].get_header("Soapaction"), '"FetchResultSet"')

    def test_execute_requires_cursor_id(self):
        execute_response = (
            '<Response><ResultSetMetadata><Column><ColumnLabel>n</ColumnLabel></Column></ResultSetMetadata>'
            '</Response>'
        )
        with patch("sql_test.urllib.request.urlopen", return_value=FakeResponse(execute_response)):
            with self.assertRaisesRegex(RuntimeError, "did not contain CursorId"):
                SqlClient("http://localhost/WsSQL2/", 10).execute("SELECT n")

    def test_execute_timed_sums_fetch_execute_ms(self):
        execute_response = (
            '<Response><ResultSetMetadata><Column><ColumnLabel>n</ColumnLabel></Column></ResultSetMetadata>'
            '<CursorId>cursor-1</CursorId><PrepareMs>3</PrepareMs></Response>'
        )
        first_fetch = '<Response><RowsJSON>[[1]]</RowsJSON><Complete>false</Complete><ExecuteMs>12.5</ExecuteMs></Response>'
        last_fetch = '<Response><RowsJSON>[[2]]</RowsJSON><Complete>true</Complete><ExecuteMs>900</ExecuteMs></Response>'
        with patch("sql_test.urllib.request.urlopen",
                   side_effect=[FakeResponse(execute_response), FakeResponse(first_fetch),
                                FakeResponse(last_fetch)]):
            result, seconds = SqlClient("http://localhost/WsSQL2/", 10).execute_timed("SELECT n")

        self.assertEqual(result, {"columns": ["n"], "rows": [[1], [2]]})
        self.assertAlmostEqual(seconds, 0.9125)

        missing = '<Response><RowsJSON>[[1]]</RowsJSON><Complete>true</Complete></Response>'
        with patch("sql_test.urllib.request.urlopen",
                   side_effect=[FakeResponse(execute_response), FakeResponse(missing)]):
            with self.assertRaisesRegex(RuntimeError, "did not contain ExecuteMs"):
                SqlClient("http://localhost/WsSQL2/", 10).execute_timed("SELECT n")

    def test_execute_reports_service_fault(self):
        fault = '<Response><Exception>query rejected</Exception></Response>'
        with patch("sql_test.urllib.request.urlopen", return_value=FakeResponse(fault)):
            with self.assertRaisesRegex(RuntimeError, "query rejected"):
                SqlClient("http://localhost/WsSQL2/", 10).execute("SELECT invalid")

    def test_execute_reports_http_soap_fault_details(self):
        body = b"<Envelope><Fault><faultstring>Wrong WsSQL2 port</faultstring></Fault></Envelope>"
        error = urllib.error.HTTPError("http://localhost/WsSQL2/", 500, "Internal Server Error", {}, None)
        error.read = lambda: body
        with patch("sql_test.urllib.request.urlopen", side_effect=error):
            with self.assertRaisesRegex(RuntimeError, "Wrong WsSQL2 port"):
                SqlClient("http://localhost/WsSQL2/", 10).execute("SELECT 1")

    def test_execute_closes_cursor_when_response_rows_are_invalid(self):
        execute_response = (
            '<Response><ResultSetMetadata><Column><ColumnLabel>n</ColumnLabel></Column></ResultSetMetadata>'
            '<CursorId>cursor-1</CursorId></Response>'
        )
        fetch_response = '<Response><RowsJSON>not-json</RowsJSON><Complete>false</Complete></Response>'
        close_response = "<Response></Response>"
        with patch("sql_test.urllib.request.urlopen",
                   side_effect=[FakeResponse(execute_response), FakeResponse(fetch_response),
                                FakeResponse(close_response)]) as urlopen:
            with self.assertRaisesRegex(RuntimeError, "Invalid Rows JSON"):
                SqlClient("http://localhost/WsSQL2/", 10).execute("SELECT n")

        self.assertEqual(urlopen.call_count, 3)
        self.assertEqual(urlopen.call_args_list[2].args[0].get_header("Soapaction"), '"CloseResultSet"')

    def test_execute_closes_cursor_after_result_metadata_validation_failure(self):
        execute_response = '<Response><CursorId>cursor-1</CursorId></Response>'
        close_response = "<Response><Closed>true</Closed></Response>"
        with patch("sql_test.urllib.request.urlopen",
                   side_effect=[FakeResponse(execute_response), FakeResponse(close_response)]) as urlopen:
            with self.assertRaisesRegex(RuntimeError, "did not contain result metadata"):
                SqlClient("http://localhost/WsSQL2/", 10).execute("SELECT 1")

        self.assertEqual(urlopen.call_count, 2)
        close_request = urlopen.call_args_list[1].args[0]
        self.assertEqual(close_request.get_header("Soapaction"), '"CloseResultSet"')
        self.assertIn(b"<CursorId>cursor-1</CursorId>", close_request.data)

    def test_execute_query_results_update_and_comparison(self):
        with tempfile.TemporaryDirectory() as suite:
            sql_dir = os.path.join(suite, "sql")
            results_dir = os.path.join(sql_dir, "ExecuteQuery")
            os.makedirs(sql_dir)
            query_path = os.path.join(sql_dir, "sample.sql")
            with open(query_path, "w", encoding="utf-8") as file:
                file.write("SELECT 1")
            result = {"columns": ["value"], "rows": [[1]]}
            client = FakeClient(result)

            run_query(client, query_path, sql_dir, results_dir, ignore_result=False, update_results=True)
            with open(os.path.join(results_dir, "sample.json"), encoding="utf-8") as file:
                self.assertEqual(json.load(file), result)
            run_query(client, query_path, sql_dir, results_dir, ignore_result=False, update_results=False)

            with self.assertRaises(AssertionError):
                run_query(FakeClient({"columns": ["value"], "rows": [[2]]}),
                          query_path, sql_dir, results_dir, ignore_result=False, update_results=False)


if __name__ == "__main__":
    unittest.main()
