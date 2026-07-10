#!/usr/bin/env python3

import argparse
import json
import sys


def format_lines(lines, limit=8):
    if len(lines) <= limit:
        return ", ".join(map(str, lines))
    head = ", ".join(map(str, lines[:limit]))
    return f"{head}, ... (+{len(lines) - limit} more)"


def lines_suffix(lines):
    if not lines:
        return "(line(s) unknown)"
    return f"(line(s) {format_lines(lines)})"


def print_limited(items, printer, max_items):
    for idx, item in enumerate(items):
        if idx >= max_items:
            break
        printer(item)

def load_events_with_lines(file_path):
    events = []
    try:
        with open(file_path, 'r') as f:
            content = f.read()

        start_idx = content.find('"Event"')
        if start_idx == -1:
            return events

        array_start = content.find('[', start_idx)
        if array_start == -1:
            return events

        brace_level = 0
        in_string = False
        escape = False
        current_obj_start = -1
        current_line = 1
        current_obj_line = 1

        for i in range(array_start, len(content)):
            c = content[i]
            if c == '\n':
                current_line += 1
            if escape:
                escape = False
                continue
            if c == '\\':
                escape = True
                continue
            if c == '"':
                in_string = not in_string
                continue

            if not in_string:
                if c == '{':
                    if brace_level == 0:
                        current_obj_start = i
                        current_obj_line = current_line
                    brace_level += 1
                elif c == '}':
                    brace_level -= 1
                    if brace_level == 0 and current_obj_start != -1:
                        obj_str = content[current_obj_start:i+1]
                        except json.JSONDecodeError:
                            print(f"Warning: failed to parse JSON event object starting at line {current_obj_line}", file=sys.stderr)
                elif c == ']' and brace_level == 0:
                    break
    except Exception as e:
        print(f"Failed to read file: {e}", file=sys.stderr)

    return events

def validate_events(file_path, verbose=False):
    print(f"Loading '{file_path}'...")
    events = load_events_with_lines(file_path)

    if events is None or not events:
        print("Warning: No events found in the file.")
        return False

    # Trackers mapping identity -> list of line numbers
    request_sends = {}
    request_receives = {}
    response_sends = {}
    response_receives = {}
    requests_with_eof = set() # Store (req_id, req_seq)
    request_send_instances = []
    task_begins_by_thread = {}
    task_ends_by_thread = {}

    for event in events:
        name = event.get("name")
        line = event.get("__line__", "?")
        req_id = event.get("RequestId")
        req_seq = event.get("RequestSeq")
        resp_id = event.get("ResponseId")
        resp_seq = event.get("ResponseSeq")
        thread_id = event.get("EventThreadId")
        trace_id = event.get("EventTraceId")

        if name == "RequestSend":
            request_sends.setdefault((req_id, req_seq), []).append(line)
            request_send_instances.append(
                {
                    "request_id": req_id,
                    "request_seq": req_seq,
                    "line": line,
                    "thread_id": thread_id,
                    "trace_id": trace_id,
                }
            )

        elif name == "RequestReceive":
            request_receives.setdefault((req_id, req_seq), []).append(line)

        elif name == "ResponseSend":
            if req_id is not None and req_seq is not None and resp_id is not None and resp_seq is not None:
                response_sends.setdefault((req_id, req_seq, resp_id, resp_seq), []).append(line)

                # Check top bit set
                if resp_seq & 0x80000000:
                    requests_with_eof.add((req_id, req_seq))

        elif name == "ResponseReceive":
            if req_id is not None and req_seq is not None and resp_id is not None and resp_seq is not None:
                response_receives.setdefault((req_id, req_seq, resp_id, resp_seq), []).append(line)

        elif name == "TaskStart":
            task_begins_by_thread.setdefault(thread_id, []).append(
                {
                    "line": line,
                    "trace_id": trace_id,
                }
            )
        elif name == "TaskStop":
            task_ends_by_thread.setdefault(thread_id, []).append(
                {
                    "line": line,
                    "trace_id": trace_id,
                }
            )

    unmatched_request_send_taskbegin = [
        req for req in request_send_instances
        if req.get("thread_id") not in task_begins_by_thread
    ]

    req_sends_keys = set(request_sends.keys())
    req_recvs_keys = set(request_receives.keys())
    missing_req_receives = req_sends_keys - req_recvs_keys
    orphaned_req_receives = req_recvs_keys - req_sends_keys
    req_shared_keys = req_sends_keys & req_recvs_keys
    unmatched_request_sends = [
        key for key in req_sends_keys
        if len(request_sends[key]) > len(request_receives.get(key, []))
    ]
    non_1to1_requests = [
        key for key in req_shared_keys
        if len(request_sends[key]) != 1 or len(request_receives[key]) != 1
    ]

    resp_sends_keys = set(response_sends.keys())
    resp_recvs_keys = set(response_receives.keys())
    missing_resp_receives = resp_sends_keys - resp_recvs_keys
    orphaned_resp_receives = resp_recvs_keys - resp_sends_keys
    resp_shared_keys = resp_sends_keys & resp_recvs_keys
    unmatched_response_sends = [
        key for key in resp_sends_keys
        if len(response_sends[key]) > len(response_receives.get(key, []))
    ]
    non_1to1_responses = [
        key for key in resp_shared_keys
        if len(response_sends[key]) != 1 or len(response_receives[key]) != 1
    ]

    missing_eof = req_recvs_keys - requests_with_eof

    unmatched_task_begins = [
        thread_id for thread_id, begins in task_begins_by_thread.items()
        if len(begins) > len(task_ends_by_thread.get(thread_id, []))
    ]
    unmatched_task_ends = [
        thread_id for thread_id, ends in task_ends_by_thread.items()
        if len(ends) > len(task_begins_by_thread.get(thread_id, []))
    ]
    task_begin_count = sum(len(begins) for begins in task_begins_by_thread.values())
    task_end_count = sum(len(ends) for ends in task_ends_by_thread.values())

    all_passed = True
    max_report_items = 100

    tests = [
        {
            "name": "Check all request sends have a corresponding request receive",
            "passed": not missing_req_receives,
            "details": lambda: _print_missing_request_receive_details(
                missing_req_receives, request_sends, max_report_items
            ),
        },
        {
            "name": "Check all request receives have a corresponding request send",
            "passed": not orphaned_req_receives,
            "details": lambda: _print_orphaned_request_receive_details(
                orphaned_req_receives, request_receives, max_report_items
            ),
        },
        {
            "name": "Check all response sends have a corresponding response receive",
            "passed": not missing_resp_receives,
            "details": lambda: _print_missing_response_receive_details(
                missing_resp_receives, response_sends, max_report_items
            ),
        },
        {
            "name": "Check all response receives have a corresponding response send",
            "passed": not orphaned_resp_receives,
            "details": lambda: _print_orphaned_response_receive_details(
                orphaned_resp_receives, response_receives, max_report_items
            ),
        },
        {
            "name": "Check request send/receive key presence symmetry",
            "passed": not missing_req_receives and not orphaned_req_receives,
            "details": lambda: _print_request_presence_details(
                missing_req_receives, orphaned_req_receives, request_sends, request_receives, max_report_items
            ),
        },
        {
            "name": "Check every request send instance is matched by a request receive instance",
            "passed": not unmatched_request_sends,
            "details": lambda: _print_request_send_coverage_details(
                unmatched_request_sends, request_sends, request_receives, max_report_items
            ),
        },
        {
            "name": "Check request send and request receive are one-to-one for matched request keys",
            "passed": not non_1to1_requests,
            "details": lambda: _print_request_1to1_details(
                non_1to1_requests, request_sends, request_receives, max_report_items
            ),
        },
        {
            "name": "Check every response send instance is matched by a response receive instance",
            "passed": not unmatched_response_sends,
            "details": lambda: _print_response_send_coverage_details(
                unmatched_response_sends, response_sends, response_receives, max_report_items
            ),
        },
        {
            "name": "Check response send and response receive are one-to-one for matched response keys",
            "passed": not non_1to1_responses,
            "details": lambda: _print_response_1to1_details(
                non_1to1_responses, response_sends, response_receives, max_report_items
            ),
        },
        {
            "name": "Check each request receive has at least one EOF response send",
            "passed": not missing_eof,
            "details": lambda: _print_eof_details(missing_eof, request_receives, max_report_items),
        },
        {
            "name": "Check each request send has a corresponding task start with matching thread id",
            "passed": not unmatched_request_send_taskbegin,
            "details": lambda: _print_request_send_taskbegin_details(
                unmatched_request_send_taskbegin, task_begins_by_thread, max_report_items
            ),
        },
        {
            "name": "Check each TaskStart has a corresponding TaskStop with matching thread id",
            "passed": not unmatched_task_begins,
            "details": lambda: _print_taskbegin_taskend_coverage_details(
                unmatched_task_begins, task_begins_by_thread, task_ends_by_thread, max_report_items
            ),
        },
        {
            "name": "Check each TaskStop has a corresponding TaskStart with matching thread id",
            "passed": not unmatched_task_ends,
            "details": lambda: _print_taskend_taskbegin_coverage_details(
                unmatched_task_ends, task_begins_by_thread, task_ends_by_thread, max_report_items
            ),
        },
        {
            "name": "Check total TaskStart count matches total TaskStop count",
            "passed": task_begin_count == task_end_count,
            "details": lambda: _print_task_count_details(task_begin_count, task_end_count),
        },
    ]

    print("\n" + "=" * 80)
    print("ROXIE TRACE VALIDATION REPORT")
    print("=" * 80)

    for test in tests:
        status = "PASS" if test["passed"] else "FAIL"
        print(f"[{status}] {test['name']}")
        if not test["passed"]:
            all_passed = False
            if verbose:
                test["details"]()

    if not verbose and not all_passed:
        print("Use -v/--verbose to show mismatch details.")

    print("=" * 80)
    return all_passed


def _print_request_presence_details(missing_req_receives, orphaned_req_receives, request_sends, request_receives, max_report_items):
    print(f"    Missing RequestReceive count: {len(missing_req_receives)}")
    print(f"    Orphaned RequestReceive count: {len(orphaned_req_receives)}")
    missing_req_receives_sorted = sorted(missing_req_receives)
    orphaned_req_receives_sorted = sorted(orphaned_req_receives)
    print_limited(
        missing_req_receives_sorted,
        lambda req: print(
            f"    Missing RequestReceive for sent Request -> RequestId: {req[0]}, Seq: {req[1]} "
            f"(Sent {lines_suffix(request_sends[req])})"
        ),
        max_report_items,
    )
    print_limited(
        orphaned_req_receives_sorted,
        lambda req: print(
            f"    Orphaned RequestReceive (was never sent) -> RequestId: {req[0]}, Seq: {req[1]} "
            f"(Received {lines_suffix(request_receives[req])})"
        ),
        max_report_items,
    )
    if len(missing_req_receives_sorted) > max_report_items:
        print(f"    ... {len(missing_req_receives_sorted) - max_report_items} more missing RequestReceive items omitted")
    if len(orphaned_req_receives_sorted) > max_report_items:
        print(f"    ... {len(orphaned_req_receives_sorted) - max_report_items} more orphaned RequestReceive items omitted")


def _print_missing_request_receive_details(missing_req_receives, request_sends, max_report_items):
    print(f"    Missing RequestReceive count: {len(missing_req_receives)}")
    missing_req_receives_sorted = sorted(missing_req_receives)
    print_limited(
        missing_req_receives_sorted,
        lambda req: print(
            f"    Missing RequestReceive for sent Request -> RequestId: {req[0]}, Seq: {req[1]} "
            f"(Sent {lines_suffix(request_sends[req])})"
        ),
        max_report_items,
    )
    if len(missing_req_receives_sorted) > max_report_items:
        print(f"    ... {len(missing_req_receives_sorted) - max_report_items} more missing RequestReceive items omitted")


def _print_orphaned_request_receive_details(orphaned_req_receives, request_receives, max_report_items):
    print(f"    Orphaned RequestReceive count: {len(orphaned_req_receives)}")
    orphaned_req_receives_sorted = sorted(orphaned_req_receives)
    print_limited(
        orphaned_req_receives_sorted,
        lambda req: print(
            f"    Orphaned RequestReceive (was never sent) -> RequestId: {req[0]}, Seq: {req[1]} "
            f"(Received {lines_suffix(request_receives[req])})"
        ),
        max_report_items,
    )
    if len(orphaned_req_receives_sorted) > max_report_items:
        print(f"    ... {len(orphaned_req_receives_sorted) - max_report_items} more orphaned RequestReceive items omitted")


def _print_request_send_coverage_details(unmatched_request_sends, request_sends, request_receives, max_report_items):
    print(f"    Request keys with unmatched sends: {len(unmatched_request_sends)}")
    unmatched_request_sends_sorted = sorted(unmatched_request_sends)
    print_limited(
        unmatched_request_sends_sorted,
        lambda req: print(
            f"    RequestId: {req[0]}, Seq: {req[1]} -> "
            f"RequestSend count={len(request_sends[req])} {lines_suffix(request_sends[req])}; "
            f"RequestReceive count={len(request_receives.get(req, []))} {lines_suffix(request_receives.get(req, []))}"
        ),
        max_report_items,
    )
    if len(unmatched_request_sends_sorted) > max_report_items:
        print(f"    ... {len(unmatched_request_sends_sorted) - max_report_items} more request keys with unmatched sends omitted")


def _print_request_1to1_details(non_1to1_requests, request_sends, request_receives, max_report_items):
    print(f"    Non 1:1 Request key count: {len(non_1to1_requests)}")
    non_1to1_requests_sorted = sorted(non_1to1_requests)
    print_limited(
        non_1to1_requests_sorted,
        lambda req: print(
            f"    RequestId: {req[0]}, Seq: {req[1]} -> "
            f"RequestSend count={len(request_sends[req])} {lines_suffix(request_sends[req])}; "
            f"RequestReceive count={len(request_receives[req])} {lines_suffix(request_receives[req])}"
        ),
        max_report_items,
    )
    if len(non_1to1_requests_sorted) > max_report_items:
        print(f"    ... {len(non_1to1_requests_sorted) - max_report_items} more non 1:1 Request keys omitted")


def _print_response_presence_details(missing_resp_receives, orphaned_resp_receives, response_sends, response_receives, max_report_items):
    print(f"    Missing ResponseReceive count: {len(missing_resp_receives)}")
    print(f"    Orphaned ResponseReceive count: {len(orphaned_resp_receives)}")
    missing_resp_receives_sorted = sorted(missing_resp_receives)
    orphaned_resp_receives_sorted = sorted(orphaned_resp_receives)
    print_limited(
        missing_resp_receives_sorted,
        lambda resp: print(
            f"    Missing ResponseReceive for -> Req: {resp[0]}, ReqSeq: {resp[1]}, Resp: {resp[2]}, RespSeq: {resp[3]} "
            f"(Sent {lines_suffix(response_sends[resp])})"
        ),
        max_report_items,
    )
    print_limited(
        orphaned_resp_receives_sorted,
        lambda resp: print(
            f"    Orphaned ResponseReceive for-> Req: {resp[0]}, ReqSeq: {resp[1]}, Resp: {resp[2]}, RespSeq: {resp[3]} "
            f"(Received {lines_suffix(response_receives[resp])})"
        ),
        max_report_items,
    )
    if len(missing_resp_receives_sorted) > max_report_items:
        print(f"    ... {len(missing_resp_receives_sorted) - max_report_items} more missing ResponseReceive items omitted")
    if len(orphaned_resp_receives_sorted) > max_report_items:
        print(f"    ... {len(orphaned_resp_receives_sorted) - max_report_items} more orphaned ResponseReceive items omitted")


def _print_missing_response_receive_details(missing_resp_receives, response_sends, max_report_items):
    print(f"    Missing ResponseReceive count: {len(missing_resp_receives)}")
    missing_resp_receives_sorted = sorted(missing_resp_receives)
    print_limited(
        missing_resp_receives_sorted,
        lambda resp: print(
            f"    Missing ResponseReceive for -> Req: {resp[0]}, ReqSeq: {resp[1]}, Resp: {resp[2]}, RespSeq: {resp[3]} "
            f"(Sent {lines_suffix(response_sends[resp])})"
        ),
        max_report_items,
    )
    if len(missing_resp_receives_sorted) > max_report_items:
        print(f"    ... {len(missing_resp_receives_sorted) - max_report_items} more missing ResponseReceive items omitted")


def _print_orphaned_response_receive_details(orphaned_resp_receives, response_receives, max_report_items):
    print(f"    Orphaned ResponseReceive count: {len(orphaned_resp_receives)}")
    orphaned_resp_receives_sorted = sorted(orphaned_resp_receives)
    print_limited(
        orphaned_resp_receives_sorted,
        lambda resp: print(
            f"    Orphaned ResponseReceive for-> Req: {resp[0]}, ReqSeq: {resp[1]}, Resp: {resp[2]}, RespSeq: {resp[3]} "
            f"(Received {lines_suffix(response_receives[resp])})"
        ),
        max_report_items,
    )
    if len(orphaned_resp_receives_sorted) > max_report_items:
        print(f"    ... {len(orphaned_resp_receives_sorted) - max_report_items} more orphaned ResponseReceive items omitted")


def _print_response_send_coverage_details(unmatched_response_sends, response_sends, response_receives, max_report_items):
    print(f"    Response keys with unmatched sends: {len(unmatched_response_sends)}")
    unmatched_response_sends_sorted = sorted(unmatched_response_sends)
    print_limited(
        unmatched_response_sends_sorted,
        lambda resp: print(
            f"    Req: {resp[0]}, ReqSeq: {resp[1]}, Resp: {resp[2]}, RespSeq: {resp[3]} -> "
            f"ResponseSend count={len(response_sends[resp])} {lines_suffix(response_sends[resp])}; "
            f"ResponseReceive count={len(response_receives.get(resp, []))} {lines_suffix(response_receives.get(resp, []))}"
        ),
        max_report_items,
    )
    if len(unmatched_response_sends_sorted) > max_report_items:
        print(f"    ... {len(unmatched_response_sends_sorted) - max_report_items} more response keys with unmatched sends omitted")


def _print_response_1to1_details(non_1to1_responses, response_sends, response_receives, max_report_items):
    print(f"    Non 1:1 Response key count: {len(non_1to1_responses)}")
    non_1to1_responses_sorted = sorted(non_1to1_responses)
    print_limited(
        non_1to1_responses_sorted,
        lambda resp: print(
            f"    Req: {resp[0]}, ReqSeq: {resp[1]}, Resp: {resp[2]}, RespSeq: {resp[3]} -> "
            f"ResponseSend count={len(response_sends[resp])} {lines_suffix(response_sends[resp])}; "
            f"ResponseReceive count={len(response_receives[resp])} {lines_suffix(response_receives[resp])}"
        ),
        max_report_items,
    )
    if len(non_1to1_responses_sorted) > max_report_items:
        print(f"    ... {len(non_1to1_responses_sorted) - max_report_items} more non 1:1 Response keys omitted")


def _print_eof_details(missing_eof, request_receives, max_report_items):
    print(f"    Missing EOF count: {len(missing_eof)}")
    missing_eof_sorted = sorted(missing_eof)
    print_limited(
        missing_eof_sorted,
        lambda req: print(
            f"    No EOF ResponseSend (top-bit set) for RequestReceive -> RequestId: {req[0]}, Seq: {req[1]} "
            f"(Received {lines_suffix(request_receives[req])})"
        ),
        max_report_items,
    )
    if len(missing_eof_sorted) > max_report_items:
        print(f"    ... {len(missing_eof_sorted) - max_report_items} more EOF-missing items omitted")


def _print_request_send_taskbegin_details(unmatched_request_send_taskbegin, task_begins_by_thread, max_report_items):
    print(f"    RequestSend events without matching TaskStart thread: {len(unmatched_request_send_taskbegin)}")
    unmatched_sorted = sorted(
        unmatched_request_send_taskbegin,
        key=lambda req: (
            req.get("request_id") if req.get("request_id") is not None else -1,
            req.get("request_seq") if req.get("request_seq") is not None else -1,
            req.get("line") if req.get("line") is not None else -1,
        ),
    )
    print_limited(
        unmatched_sorted,
        lambda req: print(
            f"    RequestSend -> RequestId: {req.get('request_id')}, Seq: {req.get('request_seq')}, "
            f"ThreadId: {req.get('thread_id')}, line: {req.get('line')}"
        ),
        max_report_items,
    )
    if len(unmatched_sorted) > max_report_items:
        print(f"    ... {len(unmatched_sorted) - max_report_items} more RequestSend without matching TaskBegin thread omitted")

    # Helpful hint when no TaskBegin events were present at all.
    if not task_begins_by_thread:
        print("    Note: No TaskStart events were found in the trace.")


def _print_taskbegin_taskend_coverage_details(unmatched_task_begins, task_begins_by_thread, task_ends_by_thread, max_report_items):
    print(f"    ThreadIds with unmatched TaskStart count: {len(unmatched_task_begins)}")
    unmatched_sorted = sorted(
        unmatched_task_begins,
        key=lambda thread_id: (thread_id is None, thread_id),
    )
    print_limited(
        unmatched_sorted,
        lambda thread_id: print(
            f"    ThreadId: {thread_id} -> "
            f"TaskStart count={len(task_begins_by_thread[thread_id])} "
            f"{lines_suffix([entry.get('line') for entry in task_begins_by_thread[thread_id]])}; "
            f"TaskStop count={len(task_ends_by_thread.get(thread_id, []))} "
            f"{lines_suffix([entry.get('line') for entry in task_ends_by_thread.get(thread_id, [])])}"
        ),
        max_report_items,
    )
    if len(unmatched_sorted) > max_report_items:
        print(f"    ... {len(unmatched_sorted) - max_report_items} more thread ids with unmatched TaskStart omitted")


def _print_taskend_taskbegin_coverage_details(unmatched_task_ends, task_begins_by_thread, task_ends_by_thread, max_report_items):
    print(f"    ThreadIds with unmatched TaskStop count: {len(unmatched_task_ends)}")
    unmatched_sorted = sorted(
        unmatched_task_ends,
        key=lambda thread_id: (thread_id is None, thread_id),
    )
    print_limited(
        unmatched_sorted,
        lambda thread_id: print(
            f"    ThreadId: {thread_id} -> "
            f"TaskStop count={len(task_ends_by_thread[thread_id])} "
            f"{lines_suffix([entry.get('line') for entry in task_ends_by_thread[thread_id]])}; "
            f"TaskStart count={len(task_begins_by_thread.get(thread_id, []))} "
            f"{lines_suffix([entry.get('line') for entry in task_begins_by_thread.get(thread_id, [])])}"
        ),
        max_report_items,
    )
    if len(unmatched_sorted) > max_report_items:
        print(f"    ... {len(unmatched_sorted) - max_report_items} more thread ids with unmatched TaskStop omitted")


def _print_task_count_details(task_begin_count, task_end_count):
    print(f"    TaskStart total count: {task_begin_count}")
    print(f"    TaskStop total count: {task_end_count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Validate request/response event pairing in an HPCC event trace file."
    )
    parser.add_argument("event_file", help="Path to events JSON file")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Display mismatch details for failed checks",
    )
    args = parser.parse_args()

    success = validate_events(args.event_file, verbose=args.verbose)
    sys.exit(0 if success else 1)
