# HPCC Systems 9.8.x Releases

Welcome to the Red Book for HPCC Systems® 9.8.x series.

>**NOTE:** You can plan any work that may be needed for the Regex changes before upgrading.

You may benefit from glancing at other Red Book entries when making a large jump between releases.

Here's how to contact us if you find an issue or want to add something to the Red Book:

- To raise an issue, use our [Community Issue Tracker](https://hpccsystems.atlassian.net/jira/). Please create yourself an account if you don't already have one, to get automatic updates as your issue progresses through the workflow.
- To ask a developer a technical question about something you are doing or have encountered, post in the Forums on Stack Overflow. We invite you to post your questions on Stack Overflow utilizing the tag **hpcc-ecl** ([StackOverflow](https://stackoverflow.com/search?tab=newest&q=hpcc-ecl)).
- To add a note to the RedBook, please contact [Jim DeFabia](mailto:james.defabia@lexisnexisrisk.com).

## Change in Regex Support Library

Starting with 9.8, we now use PCRE2 instead of Boost and ICU. All the libraries are powerful regular expression engines, but they have some key differences that make PCRE2 a better choice for the platform.

There are some differences you should be aware of with the switchover:

- Perl, in default mode, ignores invalid escape sequences (in warn or strict mode it throws an error). Boost::regex emulates Perl in default mode, but PCRE2 has no lax/warn/strict mode at all, so it defaults to strict.  
  `\I` for example is not a valid command. Perl and Boost::regex both would ignore it, but PCRE2 will throw an error.
- POSIX defines character classes like `[[:upper:]]` to mean "uppercase ASCII letters." The Unicode version of a character class is called a property name, and property names come in short and long versions. ICU supported both but PCRE2 supports only the short versions.
- The short version of the property name "uppercase letter" is `\p{Lu}` — which is written as `'\\p{Lu}'` in ECL. ICU has a custom extension that allows you to use short property names with POSIX-style syntax: `[[:Lu:]]`. That syntax is not supported by PCRE2.
- When referring to numbered capture groups in a replacement string, backslash syntax is supported by Boost but not supported by PCRE2.
  
  ```ecl
  REGEXREPLACE('(.a)t', 'the cat sat on the mat', '\\1p');
  ```

  The \\1 (which is really \1 after parsing) portion refers to the found text from the first group defined by parenthesis.

  The dollar sign (`$`) syntax works in both Boost and PCRE2.
  
  ```ecl
  REGEXREPLACE('(.a)t', 'the cat sat on the mat', '$1p');
  ```

  For more information see: [Replace Backreferences](https://www.regular-expressions.info/replacebackref.html).

- The old Regex libraries allowed illegal "bare" POSIX character class names but PCRE2 does not.  
  
  For example:  
  
  ```ecl
  //If you have this: 
  REGEXFIND('[:alpha:]', 'Whatever');

  //You should now use this: 
  REGEXFIND('[[:alpha:]]', 'Whatever');
  ```

The first item is actually beneficial because, more than likely, it produced a silent error. The other three items could require code changes to make existing regex patterns compatible with PCRE2, but the good news is that the changes are compatible with Boost::regex and ICU already. You can make the changes in your code now, before upgrading the platform to 9.8.

For more detailed information, see [Changes to Regex Support in HPCC Systems 9.8.0](Changes-to-Regex-Support-in-HPCC-Systems-9.8.0.md).

## Change in LDAP Support

Beginning with 9.8.0, a change in LDAP support may affect your cluster. This section only applies to clusters using the LDAP security manager plugin with caching enabled. Additionally, it only applies if you are using default file scope permissions to grant access to file scopes that are not managed.

Before this release, the security manager cached results inconsistently; this release addresses that. The effect was that incorrect access permissions may have been returned for a user, thus incorrectly denying or granting access to a file scope. This problem has been fixed and all users are now correctly granted or denied access based on their assigned default file scope permissions.

The problem may have resulted in unpredictable results when running workunits. In some cases, workunits may have failed with a file scope denial when they previously completed without error. The problem was the incorrect application of default file scope permissions.

This release fixes the incorrect application of cached default file scope permissions. Results for workunits are now predictable and repeatable for default file scope permissions. Before this fix, workload ordering could have affected results.

If your workflow depends on default file scope permissions to grant access, any failures are real and must be fixed. Either update the default file scope permissions using your current deployment permissions strategy or add the denied file scope as a managed file scope and add permissions as needed.

If you depend on the `filesDefaultUser` setting in the Dali LDAP configuration, this fix may also impact your workunits. Please make sure that your defined default user has the correct default permissions assigned. The `filesDefaultUser` has been deprecated and may be disabled or removed in a future release.
