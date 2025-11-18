# HPCC Systems 10.0.x Releases

Welcome to the Red Book for HPCC Systems® 10.0.x series.

You may benefit from glancing at other Red Book entries when making a large jump between releases.

Here's how to contact us if you find an issue or want to add something to the Red Book:

- To raise an issue, use our [Community Issue Tracker](https://hpccsystems.atlassian.net/jira/). Please create yourself an account if you don't already have one, to get automatic updates as your issue progresses through the workflow.
- To ask a developer a technical question about something you are doing or have encountered, post in the Forums on Stack Overflow. We invite you to post your questions on Stack Overflow utilizing the tag **hpcc-ecl** ([StackOverflow](https://stackoverflow.com/search?tab=newest&q=hpcc-ecl)).
- To add a note to the RedBook, please contact [Jim DeFabia](mailto:james.defabia@lexisnexisrisk.com) with full details.

---

## New Std.OpenSSL library

We have replaced the legacy `Std.Crypto` library with the robust `Std.OpenSSL` library, providing better performance, enhanced security algorithms, and improved maintainability.

For more details, see: [OpenSSL Migration Guide](https://hpcc-systems.github.io/HPCC-Platform/devdoc/userdoc/Migrating-from-StdCrypto-to-StdOpenSSL.html)

---

## Strengthened Security & Authentication

- **Improved LDAP Integration**: Enhanced LDAP authentication mechanisms with improved error handling and connection stability.
- **Security Fixes**: Multiple security improvements to ensure your data remains protected and compliant with enterprise standards.
