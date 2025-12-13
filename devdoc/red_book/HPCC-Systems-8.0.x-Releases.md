# HPCC Systems 8.0.x Releases

Welcome to the Red Book for HPCC Systems® 8.0.x series. There are several sections in this Red Book as follows:

- General HPCC Systems Cross Platform Issues
- Significant new features Cross Platform
- Cloud Native Platform Issues
- Significant new features Cloud Native Platform
- Getting Started with our Cloud Native Platform FAQs
- ECL IDE

Users may benefit from glancing at other Red Book entries when making a large jump between releases.

Here's how to contact us if you find an issue or want to add something to the Red Book:

- To raise an issue, use our [Community Issue Tracker](https://hpccsystems.atlassian.net/jira/). Please create yourself an account if you don't already have one, to get automatic updates as your issue progresses through the workflow.
- To ask a developer a technical question about something you are doing or have encountered, post in the Forums on Stack Overflow. We invite you to post your questions on Stack Overflow utilizing the tag **hpcc-ecl** ([StackOverflow](https://stackoverflow.com/search?tab=newest&q=hpcc-ecl)).

---

## New – HPCC Systems Cloud Native Platform

Current development on HPCC Systems includes providing a Cloud Native version of the platform. To find out how to get started, use these resources:

- Blog posts – Including [HPCC Systems 8.0.0 – Cloud Native Highlights](https://hpccsystems.com/blog/platform-Cloud-800Gold) and [Cross Platform Highlights](https://hpccsystems.com/blog/platform-BM-800Gold)
- [How To videos](../userdoc/How-to-Youtube-Library/How-to-Videos-HPCC-Systems-Cloud-Native-Platform.md)
- Links to our [Helm Chart Github Repository](https://github.com/hpcc-systems/helm-chart) and [usage examples](https://github.com/hpcc-systems/helm-chart/tree/master/helm/examples)
- [Supporting Documentation](https://hpccsystems.com/training/documentation/Containerized-Platform) and [Release Notes](https://hpccsystems.com/download/release-notes)

Our [HPCC Systems Cloud Resource Center](https://hpccsystems.com/resources/?_resource_filters=cloud) includes links to all currently available resources to help you get started, including using AKS, AWS EKS and service meshes.

Our bare metal version is [available for download on our website](https://hpccsystems.com/download#HPCC-Platform) in the usual way.

---

## General HPCC Systems Cross Platform Issues

**New – Encountering an issue where embedded Java fails**

The platform does not currently always enforce a Java Development Kit (JDK) dependency and the embedded Java plugin (`javaembed`) can fail if a JDK is not available. If you encounter this error, install the appropriate JDK for your Ubuntu distro or the `openjdk-devel` package for Centos.

[HPCC-25879](https://hpccsystems.atlassian.net/browse/HPCC-25879)

**Roxie worker threads were incorrectly given elevated priority if started on demand**

In HPCC Systems 8.0.x series, an issue which could cause CPU starvation or other difficult to predict effects has been fixed. In this case, ROXIE was not operating as intended. While there is a workaround available for earlier versions of HPCC Systems which involves setting the `prestartAgentThreads` option to true in the ROXIE configuration, it is now fixed and more details are available in this JIRA issue:

[HPCC-25493](https://hpccsystems.atlassian.net/browse/HPCC-25493)
