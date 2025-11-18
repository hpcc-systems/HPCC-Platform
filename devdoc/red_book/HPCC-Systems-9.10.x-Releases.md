# HPCC Systems 9.10.x Releases

Welcome to the Red Book for HPCC Systems® 9.10.x series.

You may benefit from glancing at other Red Book entries when making a large jump between releases.

Here's how to contact us if you find an issue or want to add something to the Red Book:

- To raise an issue, use our [Community Issue Tracker](https://hpccsystems.atlassian.net/jira/). Please create yourself an account if you don't already have one, to get automatic updates as your issue progresses through the workflow.
- To ask a developer a technical question about something you are doing or have encountered, post in the Forums on Stack Overflow. We invite you to post your questions on Stack Overflow utilizing the tag **hpcc-ecl** ([StackOverflow](https://stackoverflow.com/search?tab=newest&q=hpcc-ecl)).
- To add a note to the RedBook, please contact [Jim DeFabia](mailto:james.defabia@lexisnexisrisk.com) with full details.

---

## Simplified installers for bare metal

Before 9.10, you had the flexibility to choose and install plugins on an à la carte basis.  
We've listened to your feedback and understand that managing individual plugins could be cumbersome.  
By integrating them directly into a single platform installer, we aim to provide a more streamlined experience.

If you have a previous version of the bare metal platform installed with optional plugins, you may need to uninstall the plugins individually.

---

## Rowservice issue

We have identified a vulnerability in the Rowservice in versions 9.10.x through 9.10.14 of the platform  
that may cause the service to crash under certain conditions.

To ensure the stability and security of your systems, we strongly recommend upgrading to version 9.10.16 or later  
to mitigate the risk of crashes and ensure continued reliable operation.

If you are using earlier minor versions of the platform, you should upgrade to 9.6.84 or later or 9.8.56 or later  
([HPCC-33381](https://hpccsystems.atlassian.net/browse/HPCC-33381)).
