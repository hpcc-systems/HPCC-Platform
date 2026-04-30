# Configuring Field Translation in HPCC Platform

## Overview

Field translation controls what happens when the ECL record layout in a query
does not match the ECL record layout that was used when the file was published
to the Distributed File System (DFS). This document explains the available
configuration levels, the valid values, and their precedence.

## Configuration Levels

Field translation can be configured at three levels, listed from highest to
lowest precedence:

1. **Activity level** — applies to a single DATASET or INDEX read.
2. **Query level** — applies to all reads within a single ECL query.
3. **Global level** — applies platform-wide as a default for all queries.

### Activity Level

Use a `HINT` on the DATASET or INDEX declaration:

```ecl
myDs := DATASET('~scope::myfile', RecStruct, THOR, HINT(layoutTranslation(<value>)));
```

See [Valid Values](#valid-values) for the accepted `<value>` options.

### Query Level

Use `#OPTION` at the top of the ECL query:

```ecl
#OPTION('layoutTranslation', <value>);
```

See [Valid Values](#valid-values) for the accepted `<value>` options.

### Global Level

Set the `fieldTranslationEnabled` attribute in the environment.xml or Helm chart for each
engine that requires it. Because there is no single global setting that covers
all engines, it must be set independently for each one:

- **Thor**: add `fieldTranslationEnabled` as an attribute under `ThorCluster`.
- **hThor**: add `fieldTranslationEnabled` under the hThor definition.
- **Roxie**: add `fieldTranslationEnabled` under the Roxie definition.

## Valid Values

| Value | Behaviour |
| --- | --- |
| *(unset)* \| `"on"` \| `"true"` \| `"yes"` \| `"1"` \| `"payload"` | **Translate where possible.** Key fields of indexes cannot be translated. This is the default. |
| `"false"` \| `"no"` \| `"0"` \| *(any other non-blank value, e.g., `"off"`)* | **No translation.** If the published layout and the query layout differ, an error is thrown and the query is aborted. |
| `"alwaysECL"` \| `"ecl"` | **Force the query's ECL definition.** The published layout is ignored entirely. Use only in special cases where the ECL definition is known to be authoritative (e.g., reading a fixed-length CSV sprayed as lines with a known fixed-field overlay). Incorrect use will very likely crash queries. Since 7.12.20, using `"alwaysECL"` at query or global level issues a warning and the option is ignored — the system behaves as if translation is off. See [PR #14461](https://github.com/hpcc-systems/HPCC-Platform/pull/14461). |
| `"alwaysDisk"` \| `"disk"` | **Developer option only.** Forces translation *on* for ECL flat-disk read statements that attempt to read non-flat files. Normally translation is off in this scenario. |

## Precedence

Activity level (HINT) > Query level (#OPTION) > Global level (environment.XML or Helm chart).

## Disabling Field Translation Globally

### Bare Metal

To turn off field translation as the default for all queries, set
`fieldTranslationEnabled` to `"off"` (or any non-blank, non-affirmative value)
in the `ThorCluster`, hThor, and Roxie sections of the environment.xml.

Individual queries or activities can still override this with a `#OPTION` or
`HINT` if selective translation is needed.

### Containerized

In a containerized deployment, you can turn off field translation as the default for all queries, by setting
`fieldTranslationEnabled` to `"false"` in each engine's section in the override Helm chart.

For example:

``` yaml
thor:
  - name: thor
    prefix: thor
    numWorkers: 2
    maxJobs: 4
    maxGraphs: 2
    fieldTranslationEnabled: "false"    
```
