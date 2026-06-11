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
        for line in f:
            if re.match(r"^\s*-\s*", line):
                component = re.sub(r"^\s*-\s*", "", line).strip()
                if component:
                    components.append(component)
    return components


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

    if not allowed_components:
        fail(f"✗ No components found in impacts configuration file: {impacts_config_file}")

    errors = []

    directive_pattern = re.compile(f"{issue_close_regex}|{no_issue_regex}", re.IGNORECASE | re.MULTILINE)
    if directive_pattern.search(filtered_content):
        print(f"✓ {subject} includes a valid issue-closing directive or NoIssue")
    else:
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

    impacts_lines = re.findall(r"^Impacts:[ \t]+.*$", filtered_content, re.MULTILINE)
    if not impacts_lines:
        errors.extend(
            [
                f"✗ {subject} is missing the required Impacts line",
                "",
                "Add one line that starts with:",
                "  Impacts: Esp",
                "  Impacts: Thor, Roxie",
                "  Impacts: None",
            ]
        )

    impact_items = []
    for impacts_line in impacts_lines:
        impacts_value = impacts_line.split("Impacts:", 1)[1].strip()
        if not impacts_value:
            errors.append(f"✗ {subject} has an empty Impacts line")
            continue

        line_items = [item.strip() for item in impacts_value.split(",")]
        impact_items.extend(line_items)

    allowed_by_lower = {component.lower(): component for component in allowed_components}

    none_count = 0
    for item in impact_items:
        if not item:
            errors.append(f"✗ {subject} has an invalid Impacts list (empty component found)")
            continue

        lower_item = item.lower()
        if lower_item == "none":
            none_count += 1
            if none_count > 1:
                errors.append(f"✗ {subject} has duplicate Impacts component: None")
            continue

        canonical_item = allowed_by_lower.get(lower_item)
        if not canonical_item:
            errors.extend(
                [
                    f"✗ {subject} has an invalid Impacts component: {item}",
                    "",
                    f"Allowed components: {' '.join(allowed_components)}",
                    "Special case: None (must be the only entry)",
                ]
            )

    if none_count > 0 and len(impact_items) != 1:
        errors.extend(
            [
                f"✗ {subject} uses 'None' in Impacts with other components",
                "",
                "If 'None' is used, it must be the only Impacts entry.",
            ]
        )

    if errors:
        for line in errors:
            print(line)
        sys.exit(1)

    print(f"✓ {subject} includes a valid Impacts line")


if __name__ == "__main__":
    main()