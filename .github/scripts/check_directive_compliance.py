#!/usr/bin/env python3

import re
import sys


def fail(message):
    print(message)
    sys.exit(1)


def filter_directive_text(text):
    # Strip HTML comments (including multi-line) so hidden directives are ignored
    text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)

    filtered_lines = []
    in_fence = False

    for line in text.splitlines():
        trimmed = line.lstrip()

        if trimmed.startswith("```") or trimmed.startswith("~~~"):
            in_fence = not in_fence
            continue

        if in_fence:
            continue

        if trimmed.startswith(">"):
            continue

        filtered_lines.append(line)

    return "\n".join(filtered_lines)


def load_allowed_components(path):
    components = []
    with open(path, "r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            # Component must start at column 1 (no leading whitespace).
            # Accepts exactly one token per line, with optional trailing comment.
            # Rejects any leading whitespace, extra non-comment content, or inline spaces in the token.
            match = re.match(r"^([^\s#]+)\s*(?:#.*)?$", line)
            if not match:
                raise ValueError(
                    f"Invalid component format in {path} at line {line_number}: {line.rstrip()}"
                )

            components.append(match.group(1))

    return components


def build_impacts_guidance(impacts_config_file, allowed_components):
    return [
        "Impacts syntax (must start at column 1):",
        "  Impacts:<component>[, <component> ...][.]",
        "Rules:",
        "  - Optional spaces after ':' are allowed (Impacts:Thor or Impacts: Thor)",
        "  - Arbitrary spaces around comma-separated items are allowed",
        "  - Empty items are invalid (no double commas, no trailing comma)",
        "  - Optional trailing '.' is allowed only on the last item with no space before it",
        f"Valid components ({len(allowed_components)}): {', '.join(allowed_components)}",
        f"Configuration file: {impacts_config_file}",
        "Special case: None (must be the only entry)",
    ]


def main():
    if len(sys.argv) != 5:
        fail("✗ usage: check_directive_compliance.py <subject> <issue_close_regex> <no_issue_regex> <impacts_config_file>")

    subject = sys.argv[1]
    issue_close_regex = sys.argv[2]
    no_issue_regex = sys.argv[3]
    impacts_config_file = sys.argv[4]

    content = sys.stdin.read()
    filtered_content = filter_directive_text(content)

    try:
        allowed_components = load_allowed_components(impacts_config_file)
    except FileNotFoundError:
        fail(f"✗ Impacts configuration file not found: {impacts_config_file}")
    except ValueError as e:
        fail(f"✗ {e}")

    if not allowed_components:
        fail(f"✗ No components found in impacts configuration file: {impacts_config_file}")

    errors = []

    # Find all issue-closing directives and NoIssue directives separately
    issue_close_pattern = re.compile(issue_close_regex, re.IGNORECASE | re.MULTILINE)
    no_issue_pattern = re.compile(no_issue_regex, re.IGNORECASE | re.MULTILINE)

    issue_close_matches = issue_close_pattern.findall(filtered_content)
    no_issue_matches = no_issue_pattern.findall(filtered_content)

    total_directives = len(issue_close_matches) + len(no_issue_matches)

    if total_directives == 0:
        errors.extend(
            [
                f"✗ {subject} is missing a valid issue-closing directive or NoIssue",
                "",
                "Add one directive line in one of these formats:",
                "  Fixes #123",
                "  Closes: #456",
                "  Resolves: #789",
                "  NoIssue",
            ]
        )
    elif len(issue_close_matches) > 0 and len(no_issue_matches) > 0:
        # Conflicting directives: cannot mix NoIssue with issue-closing directives
        errors.extend(
            [
                f"✗ {subject} has conflicting directives: cannot use both issue-closing directive and NoIssue",
                "",
                "Use either:",
                "  - One or more issue-closing directives (Fixes/Closes/Resolves #123), OR",
                "  - NoIssue",
                "  (but not both)",
            ]
        )
    elif len(no_issue_matches) > 1:
        # Multiple NoIssue directives
        errors.extend(
            [
                f"✗ {subject} has multiple NoIssue directives (should have exactly one)",
                "",
                "Use only one NoIssue directive",
            ]
        )
    else:
        # Valid: either multiple issue-closing directives or single NoIssue
        print(f"INFO: {subject} includes a valid issue-closing directive or NoIssue")

    # Keep Impacts anchored at column 1, allow optional whitespace after colon.
    impacts_errors = []
    impacts_lines = re.findall(r"^Impacts:[ \t]*.*$", filtered_content, re.MULTILINE | re.IGNORECASE)
    if not impacts_lines:
        impacts_errors.extend(
            [
                f"✗ {subject} is missing the required Impacts line",
                "",
            ]
        )

    impact_items = []
    for impacts_line in impacts_lines:
        # Extract value after "Impacts:" (case-insensitive)
        match = re.match(r"^Impacts:[ \t]*(.*)$", impacts_line, re.IGNORECASE)
        impacts_value = match.group(1).strip() if match else ""
        if not impacts_value:
            impacts_errors.append(f"✗ {subject} has an empty Impacts line")
            continue

        line_items = [item.strip() for item in impacts_value.split(",")]

        # A trailing period is accepted on the final item of each Impacts line
        # only if adjacent, e.g. "Impacts: Thor." but not "Impacts: Thor .".
        if line_items:
            last_item = line_items[-1]
            if re.match(r"^.*\s\.$", last_item):
                impacts_errors.append(
                    f"✗ {subject} has an invalid Impacts component: {last_item} (remove whitespace before trailing '.')"
                )
            elif last_item.endswith("."):
                line_items[-1] = last_item[:-1]

        impact_items.extend(line_items)

    allowed_by_lower = {component.lower(): component for component in allowed_components}

    none_count = 0
    for item in impact_items:
        if not item:
            impacts_errors.append(f"✗ {subject} has an invalid Impacts list (empty component found)")
            continue

        lower_item = item.lower()
        if lower_item == "none":
            none_count += 1
            if none_count > 1:
                impacts_errors.append(f"✗ {subject} has duplicate Impacts component: None")
            continue

        canonical_item = allowed_by_lower.get(lower_item)
        if not canonical_item:
            impacts_errors.extend(
                [
                    f"✗ {subject} has an invalid Impacts component: {item}",
                ]
            )

    if none_count > 0 and len(impact_items) != 1:
        impacts_errors.extend(
            [
                f"✗ {subject} uses 'None' in Impacts with other components",
            ]
        )

    if impacts_errors:
        errors.extend(impacts_errors)
        errors.extend(["", *build_impacts_guidance(impacts_config_file, allowed_components)])

    if errors:
        for line in errors:
            print(line)
        sys.exit(1)

    print(f"INFO: {subject} includes a valid Impacts line")


if __name__ == "__main__":
    main()