# HPCC Systems 9.14.x Releases

Welcome to the Red Book for HPCC Systems® 9.14.x series.

You may benefit from glancing at other Red Book entries when making a large jump between releases.

Here's how to contact us if you find an issue or want to add something to the Red Book:

- To raise an issue, use our [Community Issue Tracker](https://hpccsystems.atlassian.net/jira/). Please create yourself an account if you don't already have one, to get automatic updates as your issue progresses through the workflow.
- To ask a developer a technical question about something you are doing or have encountered, post in the Forums on Stack Overflow. We invite you to post your questions on Stack Overflow utilizing the tag **hpcc-ecl** ([StackOverflow](https://stackoverflow.com/search?tab=newest&q=hpcc-ecl)).
- To add a note to the RedBook, please contact [Jim DeFabia](mailto:james.defabia@lexisnexisrisk.com) with full details.

---

## Security Support Improvement

(Ver. 9.14.2 and later)

Security support has improved access checking when there is a timeout accessing the Active Directory server.  
When access to the Active Directory cannot be attained, requests return an unavailable or no access allowed error message.  
Previously, access might have erroneously been allowed.

---

## Change to Std.System.Workunit.WorkunitMessages return structure

(Ver. 9.14.12 and later)

The return structure for the `Std.System.Workunit.WorkunitMessages` changed in version 9.14.12 to add two fields:  
`priority` and `cost`.

The new structure is defined as:

```ecl
EXPORT WsMessage_v2 := RECORD
  UNSIGNED4 severity;
  INTEGER4 code;
  STRING32 location;
  UNSIGNED4 row;
  UNSIGNED4 col;
  STRING16 source;
  STRING20 time;
  UNSIGNED4 priority;
  REAL8 cost;
  STRING message{MAXLENGTH(1024)};
END;
```

Two new attributes have been introduced:

- Cost: Represents the estimated financial impact (e.g., dollars) associated with the reported issue.
This attribute is only returned for Cost Optimizer messages.

- Priority: Indicates the issue's importance as a numeric value, where higher numbers represent greater significance.
For Cost Optimizer messages, the priority attribute has a different meaning:
it represents the best-case estimated time-saving potential in microseconds.
To convert this value to seconds, divide it by 1,000,000.
