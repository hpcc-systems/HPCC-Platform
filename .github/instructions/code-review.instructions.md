---
applyTo: "**"
excludeAgent: "cloud-agent"
---

# Code Review Instructions

These instructions apply when Copilot is reviewing a pull request. They govern
how to communicate findings. Language-specific checks are in the relevant
`*.instructions.md` files (e.g. `cpp.instructions.md`).

## Review Philosophy
- Post inline comments anchored to the specific line(s) where the issue exists.
- Where the fix is clear and self-contained, include an inline suggested change.
- Be concise and actionable — 1–2 sentences per comment maximum.
- Context matters: distinguish hot paths (correctness and performance critical)
  from cold paths (readability may take priority).
- Strictness should match the sensitivity of the code: core libraries and
  security-sensitive paths demand more rigour than developer-only tooling.
- If the concurrency model, performance requirements, or architectural intent are
  unclear, raise it as `discuss:` rather than assuming and flagging a false issue.
- Do not extend the scope of the original change. If an improvement is valid but
  out of scope, tag it `future:` rather than blocking the PR.
- The goal is to catch bugs, design problems, and security issues — not to rewrite
  code in a preferred style.

## PR-Level Checks (before line review)

### Commit Title & Target Branch
- Is the commit title understandable as a standalone changelog entry?
- Is the target branch correct for the type of change?
- Does the PR description explain *what* was changed and *why*?

### Issue Linkage & Scope
- If the PR links to an issue, verify the changes actually solve the described problem.
- Does the PR stay within the scope of the linked issue? Use `scope:` if it includes
  unrelated features, fixes, or refactoring.
- Is the implementation proportional to the problem? Flag over-engineering as `design:`
  if custom infrastructure was built where existing HPCC or stdlib primitives would
  suffice. Ask: could this be done in a fraction of the lines?
- Watch for wheel-reinvention of existing HPCC primitives — e.g. custom barrier or
  thread-pool implementations where `Semaphore`, `IThreadPool`, or `CThreaded` already exist.
- If the PR is over-engineered, post that `design:` verdict as the **first and most
  prominent comment**, before any line-level findings. Line-level issues on code that
  should be rewritten are noise until the design question is resolved.

### PR Size
- **< 200 lines**: Generally fine.
- **200–500 lines**: Acceptable if cohesive; watch for unrelated changes.
- **500–1000 lines**: Requires justification; consider recommending a split.
- **> 1000 lines**: Almost always should be split. Acceptable exceptions:
  - Mechanical refactoring (symbol rename across codebase)
  - Auto-generated code updates (protobuf, grammar files)
  - Dependency version bumps with cascade changes
  - A large new feature that genuinely cannot be decomposed (rare; usually itself a `design:` concern)
- Test code is not exempt — a test requiring hundreds of lines of custom infrastructure
  is itself a `design:` problem.

Beyond line count, evaluate whether the PR should be split based on:
- **File spread**: Are changes scattered across multiple unrelated components
  (e.g. `dali/`, `thorlcr/`, and `esp/` together may indicate 3 separate PRs)?
- **Change type mixing**: Does the PR bundle a bug fix, a new feature, and
  refactoring together? Are cosmetic/style changes mixed with functional ones?
- **Component boundaries**: Could each component's changes stand alone and be
  merged independently without breaking functionality?
- **Dependency analysis**: Are all changed files truly dependent on each other,
  or could some be sequenced as PR1 → PR2 → PR3?
- **Review complexity**: Would a reviewer need different expertise for different
  parts (e.g. security vs performance vs UI)? If so, splitting improves review
  quality and safety.

Use `scope:` to recommend a split, naming the suggested sub-PRs and their files. When
in doubt — if splitting is possible and would improve reviewability — recommend it.

### Test Coverage
- **New features**: should include unit or ECL tests demonstrating the functionality.
- **Bug fixes**: should include regression tests proving the bug is fixed.
- **Refactoring**: should maintain or improve existing test coverage.
- **Performance improvements**: should include benchmarks or profiling evidence where practical.
- **API changes**: should include tests for new interfaces and edge cases.

Test types to consider: unit tests (C++, for library/algorithm code), ECL tests (for language
features and runtime behaviour), integration tests (cross-component, e.g. Dali + Thor),
regression tests (for bug fixes). Look for changes in `testing/`, `**/test/`, or files
matching `*test*.cpp` / `*test*.ecl`.

If testable changes exist but no test files are touched, use `discuss:` or `future:`.

Omitting tests is acceptable for: trivial changes (typos, comments, whitespace), pure
refactoring with no observable behaviour change, changes already covered by comprehensive
existing tests, and documentation-only changes.

### Documentation
Consider whether the change affects documentation in any of these areas:

**User-facing behaviour** — use `documentation:` to flag when a follow-up doc PR is needed:
- New ECL language features or functions → user documentation required
- New command-line options or configuration keys → documentation update required
- New or changed APIs → API documentation required
- Behaviour changes in existing features → release notes and user docs

**Developer/operator concerns:**
- New deployment options → installation/deployment docs
- Configuration changes → admin guides
- Performance characteristic changes → performance tuning docs

**Scope of documentation in this PR:**
- In-scope: code comments, inline documentation explaining *why*
- Separate PR recommended: user guides, admin manuals, website content (`devdoc/` or external repos)

## Comment Tags

Prefix every review comment with one of the following tags.

### Blocking tags (must be addressed before merge)

| Tag             | Meaning                                                              |
|-----------------|----------------------------------------------------------------------|
| `design:`       | Architectural or design problem affecting functionality or extensibility |
| `scope:`        | PR scope does not match the associated issue; consider splitting     |
| `function:`     | Incorrect or unexpected functionality                                |
| `security:`     | Introduces a security vulnerability                                  |
| `bug:`          | Coding issue causing incorrect behaviour, crash, or data corruption  |

### Non-blocking tags

| Tag               | Meaning                                                            |
|-------------------|--------------------------------------------------------------------|
| `efficiency:`     | Works correctly but has scaling or performance concerns            |
| `discuss:`        | Potential issue requiring clarification before action              |
| `style:`          | Non-conforming code style                                          |
| `indent:`         | Indentation issue                                                  |
| `format:`         | Unusual formatting                                                 |
| `typo:`           | Minor typing error                                                 |
| `minor:`          | Minor improvement, unlikely to block merge                         |
| `picky:`          | Very minor, barely worth noting; developer discretion              |
| `future:`         | Valid improvement but out of scope for this PR                     |
| `question:`       | Reviewer needs clarification before deciding severity              |
| `note:`           | Information the contributor may not be aware of                    |
| `documentation:`  | Change may affect user-facing documentation; follow-up PR needed   |
| `personal:`       | Suggestion based on reviewer preference, not required              |

## Output Format

There are two distinct places where findings appear:

**Inline comments** — anchor to the specific diff line where the issue exists. Use for
all findings that relate to a specific line or block of code. Format: one concise line
`tag: Problem. Suggested fix.` Include a suggested change block when the fix is clear
and self-contained.

**Review summary body** — use for PR-level findings that are not tied to a specific line:
commit title issues, missing tests, PR size/scope concerns, over-engineering verdict,
and documentation gaps. Open the summary with any blocking `design:` or `scope:` verdict
so the author sees it first. Group the summary by severity:

**🔴 Critical** — correctness, data races, security vulnerabilities, crashes
**🟡 Important** — performance, contention, complexity, API compatibility
**🟢 Suggestions** — readability, style, minor optimisations

Close with a 1–2 sentence summary of overall code health and the primary area to
focus on.
