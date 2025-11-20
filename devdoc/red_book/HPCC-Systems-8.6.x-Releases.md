# HPCC Systems 8.6.x Releases

Welcome to the Red Book for HPCC Systems® 8.6.x series.

You may benefit from glancing at other Red Book entries when making a large jump between releases.

Here's how to contact us if you find an issue or want to add something to the Red Book:

- To raise an issue, use our [Community Issue Tracker](https://hpccsystems.atlassian.net/jira/). Please create yourself an account if you don't already have one, to get automatic updates as your issue progresses through the workflow.
- To ask a developer a technical question about something you are doing or have encountered, post in the Forums on Stack Overflow. We invite you to post your questions on Stack Overflow utilizing the tag **hpcc-ecl** ([StackOverflow](https://stackoverflow.com/search?tab=newest&q=hpcc-ecl)).
- To add a note to the RedBook, please contact [Jim DeFabia](mailto:james.defabia@lexisnexisrisk.com) with full details.

---

## Embedded C++ using the libz library

In 8.6 there is a change that affects embedded C++ code using routines from the libz library, such as the `uncompress()` routine.

Prior to 8.6, the ECL would compile and run. Using version 8.6 or later, you may get an error similar to the one below about `uncompress()` or any other libz routine used as "unresolved symbols":

```cpp
Error loading /var/lib/HPCCSystems/queries/<thorname>/libW20220728-123456.so: undefined symbol: uncompress
```

The solution is to provide an additional line into the embedded C++ code:

```cpp
#option library 'z'
