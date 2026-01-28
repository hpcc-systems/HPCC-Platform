# HPCC Systems 10.2.x Releases

Welcome to the Red Book for HPCC Systems® 10.2.x series.

You may benefit from glancing at other Red Book entries when making a large jump between releases.

Here's how to contact us if you find an issue or want to add something to the Red Book:

- To raise an issue, use our [Community Issue Tracker](https://hpccsystems.atlassian.net/jira/). Please create yourself an account if you don't already have one, to get automatic updates as your issue progresses through the workflow.
- To ask a developer a technical question about something you are doing or have encountered, post in the Forums on Stack Overflow. We invite you to post your questions on Stack Overflow utilizing the tag **hpcc-ecl** ([StackOverflow](https://stackoverflow.com/search?tab=newest&q=hpcc-ecl)).
- To add a note to the RedBook, please contact [Jim DeFabia](mailto:james.defabia@lexisnexisrisk.com) with full details.

---

## Strengthened Security & Authentication

- **Security Fixes**: Multiple security improvements to ensure your data remains protected and compliant with enterprise standards.

A recent security change [(see Jira issue)](https://hpccsystems.atlassian.net/browse/HPCC-35434) means that an HPCC Systems cluster must have a valid configured HPCC admin user in the admin group configured for the cluster. Support for the workaround flag has been removed.

You can create an HPCC Admin user in the following ways:

1. In a Kubernetes deployment, the HPCC admin user can be created using a Kubernetes or Vault secret.

2. Have Ops create the new admin user in the configured admin group.
    (Note: This may also require creating base DN OUs if not already created. These are normally created by the platform when it starts if needed, so they **must** match)
