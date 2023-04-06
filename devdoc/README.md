# Developer Documentation

This directory contains the documentation specifically targeted at developers of the HPCC system.  

::: tip
These documents are generated from Markdown by VitePress.  See [VitePress Markdown](https://vitepress.vuejs.org/guide/markdown) for more details.
:::

## General documentation

-   [Development guide](Development.md): Building the system and development guide.
-   [C++ style guide](StyleGuide.md): Style guide for c++ code.
-   [ECL style guide](/ecllibrary/StyleGuide.md): Style guide for ECL code.
-   [Wiki](https://github.com/hpcc-systems/HPCC-Platform/wiki):  GitHub wiki for HPCC-Platform
-   [Code Submission Guidelines](CodeSubmissions.md):  Guidelines for submitting PRs
-   [Code Review Guidelines](CodeReviews.md):  Guidelines for reviewing submissions PRs

## Implementation details for different parts of the system

-   [Workunit Workflow](Workunits.md): An explanation of workunits, and a walk-through of the steps in executing a query.
-   [Code Generator](CodeGenerator.md): Details of the internals of eclcc.
-   [Roxie](roxie.md): History and design details for roxie.
-   [Memory Manager](MemoryManager.md): Details of the memory manager (roxiemem) used by the query engines.
-   [Metrics](Metrics.md): Metrics Framework Design.

## Other documentation

The ECL language is documented in the ecl language reference manual (generated as ECLLanguageReference-\<version\>.pdf).
