# HPCC Systems 9.2.x Releases

Welcome to the Red Book for HPCC Systems® 9.2.x series.

You may benefit from glancing at other Red Book entries when making a large jump between releases.

Here's how to contact us if you find an issue or want to add something to the Red Book:

- To raise an issue, use our [Community Issue Tracker](https://hpccsystems.atlassian.net/jira/). Please create yourself an account if you don't already have one, to get automatic updates as your issue progresses through the workflow.
- To ask a developer a technical question about something you are doing or have encountered, post in the Forums on Stack Overflow. We invite you to post your questions on Stack Overflow utilizing the tag **hpcc-ecl** ([StackOverflow](https://stackoverflow.com/search?tab=newest&q=hpcc-ecl)).
- To add a note to the RedBook, please contact [Jim DeFabia](mailto:james.defabia@lexisnexisrisk.com) with full details.

---

## Allowed Pipe Programs

In version 9.2.0 and greater, commands used in PIPE are restricted by default. However, for legacy reasons, the default stock behavior is different in bare-metal and containerized deployments.

In both types of systems, if `allowedPipePrograms` is unset, then all but "built-in" programs are restricted (The only built-in program currently is `roxiepipe`).

In bare-metal, the default `environment.xml` includes a line:

```xml
allowedPipePrograms="*"
```

This means (by default) any PIPE program can be used.

**In a secure system, this should be removed to avoid arbitrary programs, including system programs, from being executed.**

Containerized deployments have no default setting for `allowedPipePrograms`, meaning that by default all programs except the built-in programs are blocked unless explicitly allowed.

For example:

```xml
thor:                  
- name: thor           
  prefix: thor         
  numWorkers: 2        
  maxJobs: 4           
  maxGraphs: 2         
  allowedPipePrograms: 
  - sort               
  - awk                
  - grep               
```

## New Index compression

Version 9.2.0 introduces a new index compression format. This format is only implemented when you add one of the new options for the COMPRESSED() attribute to the INDEX definition or the BUILD action.

Indexes using the new format can only be read by systems running 9.2.0 or greater or 9.0.20 or greater.
