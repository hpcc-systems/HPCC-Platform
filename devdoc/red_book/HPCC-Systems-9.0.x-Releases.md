# HPCC Systems 9.0.x Releases

Welcome to the Red Book for HPCC Systems® 9.0.x series.

You may benefit from glancing at other Red Book entries when making a large jump between releases.

Here's how to contact us if you find an issue or want to add something to the Red Book:

- To raise an issue, use our [Community Issue Tracker](https://hpccsystems.atlassian.net/jira/). Please create yourself an account if you don't already have one, to get automatic updates as your issue progresses through the workflow.
- To ask a developer a technical question about something you are doing or have encountered, post in the Forums on Stack Overflow. We invite you to post your questions on Stack Overflow utilizing the tag **hpcc-ecl** ([StackOverflow](https://stackoverflow.com/search?tab=newest&q=hpcc-ecl)).
- To add a note to the RedBook, please contact [Jim DeFabia](mailto:james.defabia@lexisnexisrisk.com) with full details.

---

## Client Tools and OpenSSL Vulnerability

Due to a vulnerability in the version 3.0 OpenSSL library prior to version 3.08, we recommend updating your Client Tools to a version using the newer library.

- If you are using a version of Client Tools from the 8.12.x series, upgrade to 8.12.26 or later.
- For the 9.0.x series, upgrade to 9.0.14 or later.
- This issue does not affect the 9.2.x series Client Tools.
- If you are using a version prior to 8.12.x, there is no reason to delay upgrading even if you are targeting an older system.

---

## Changes to Log Access Helm Chart

The Log Access Helm-based configuration has been modified as of version 9.0.0.

- The latest configuration structure includes known log column metadata entries (used to declare back-end log processor log columns).
- This is used by UI layers for optimal visualization and filtering of log data.
- These changes include the replacement of an ambiguous log column type "Host", which can result in deployment time errors when the legacy entry is encountered.

To fully utilize the new feature and avoid potential deployment time errors, anyone using Log Access in version 9.0.0 or greater should model their values' structure as depicted here:  
[loganalytics-hpcc-logaccess.yaml](https://github.com/hpcc-systems/HPCC-Platform/blob/candidate-9.0.x/helm/examples/azure/log-analytics/loganalytics-hpcc-logaccess.yaml).

## Cassandra as a System Data Store

Support for using Cassandra as a system data store is being deprecated in our next minor release. Support for Cassandra as an embedded language and database will continue as before.

---

## Spark-HPCC Connector Integration

The HPCC integrated Spark plugin is no longer supported as of version 9.0.0.

- It is replaced by stand-alone user-managed Spark clusters linked to the HPCC platform using the Spark-HPCC connector.
- If you have an existing deployment of the HPCC platform with the Spark-HPCC plugin and wish to upgrade, you will need to make some configuration changes.
- You'll only need to do this once for each environment where Spark integration was configured.

Click for [detailed instructions](How-to-Remove-the-Spark-HPCC-Connector-Integration.md) to remove the Spark-HPCC Connector integration from your environment.
