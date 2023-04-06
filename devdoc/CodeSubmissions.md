# Code Submission Guidelines

We welcome submissions to the platform especially in the form of pull requests into the HPCC-Systems github repository.  The following describes some of processes for merging PRs.

## Pull requests
There are a few things that should be considered when creating a PR to increase the likelihood that they can be accepted quickly.

* Write a good commit message\
  The format should be HPCC-XXXXX (where XXXXX is the bug number) followed by a description of the issue.  The text should make sense in a change log by itself - without reference to the jira or the contents of the PR.  We should aim to increase the information that is included as part of the commit message - not rely on on the jira.
* Ensure the reviewer has enough information to review the change.\
  The code reviewer only has the JIRA and the PR to go on.  The JIRA (or associated documentation) should contain enough details to review the PR - e.g. the purpose, main aim, why the change was made etc.. If the scope of the jira has changed then the jira should be updated to reflect that.\
  If the submission requires changes to the documentation then the JIRA should contain all the details needed to document it, and the PR should either contain the documentation changes, or a documentation JIRA should be created.
* Fill in the checklist\
  The check boxes are there to remind you to consider different aspects of the PR.  Not all of them apply to every submission, but if you tick a box and have not really thought about the item then prepare to be embarrassed!
* Prefer small submissions\
  It isn't always possible, but several smaller PRs are much easier to review than one large change.  If your submission includes semi-automatic/mechanical changes (e.g. renaming large numbers of function calls, or adding an extra parameter) please keep it as a separate commit.  This makes it much easier to review the PR - since the reviewer will be looking for different errors in the different types of changes.
* Check for silly mistakes\
  Review your own code in github, after creating the PR to check for silly mistakes.  It doesn't take long, and often catches trivial issues.  It may avoid the need for a cycle of code-review/fixes.  It may be helpful to add some notes to specific changes e.g. "this change is mainly or solely refactoring method A into method B and C. ".  Some common examples of trivial issues to look for include:
  - Inconsistent indentation, or using tabs rather than spaces to indent.
  - Lines of tracing left in.
  - Lines of code commented out that should be deleted.
  - TBD reminders of work that need implementing or removing.
  - Unrelated files that have been accidentally modified.
  - Accidental changes to submodule versions.
  - Typos in error messages, tracing or comments, or in the commit message.
  - Incomplete edits when copy and pasting code.
  - Check subtractions are the right way around, and potential for overflow.
  - New files with the wrong copyright date
* Check the target branch (see below)
* Request one or more reviews.  For relatively simple changes a single reviewer is normally enough.

## Reviewers

All pull requests should be reviewed by someone who is not the author before merging.  Complex changes, changes that require input from multiple experts, or that have implications throughout the system should be reviewed by multiple reviewers.  This should include someone who is responsible for merging changes for that part of the system.  (Unless it is a simple change written by someone with review rights.)

Contributors should use the github reviewers section on the PR to request reviews.  After a contributor has pushed a set of changes in response to a review, they should refresh the github review status, so the users are notified it is ready for re-review.  When the review is complete, a person with responsibility for merging changes to that part of the system should be added as a reviewer (or refreshed), with a comment that it is ready to merge.

Reviewers should check for PRs that are ready for their review via github's webpage (filter "review-requested:\<reviewer-id>") or via the github CLI (e.g. gh pr status).  Contributors should similarly ensure they stay up to date with any comments on requests for change on their submissions.

## Target Version

We normally maintain 4 versions of the system - which means that each new major or minor release will typically be supported for a year.  Once a new major or minor version goes gold it becomes the current version, and should not have any changes that change the behavior of queries.  PRs should target the oldest appropriate branch, and once they are merged they will be automatically up-merged into later versions.  Which branch should changes target?  The following gives some examples and illustrates the version numbers assuming 8.12.x is the latest version.

master:
- New features.
- Bug fixes that will change the semantics of existing queries or processes.
- Refactoring.
- Performance improvements (unless simple and safe)

current(8.12.x):
- Bug fixes that only change behavior where it previously crashes or had undefined behavior (If well defined but wrong need to have very strong justification to change.)
- Fixes for race conditions (the behavior was previously indeterminate so less of an argument against it changing)
- Data corruption fixes - on a case by case basis if they change existing query results.
- Missing functionality that prevents features from working.
- Changes for tech-preview work that only effect those who are using it.
- Regressions.
- Improvements to logging and error messages (possibly in "previous" if simple and added to help diagnose problems).
- Occasional simple refactoring that makes up-merging simpler..
- Changes to improve backward compatibility of new features. (E.g. adding an ignored syntax to the compiler.)
- Performance improvements - if simple and safe

previous(8.10.x):
- Simple bug fixes that do not change behavior
- Simple changes for missing functionality
- Regressions with simple fixes (but care is needed if it caused a change in behavior)
- Serious regressions
- Complex security fixes

security(8.8.x)
- Simple security fixes
- Complex security fixes if sufficiently serious

deprecated(8.6.x)
- Serious security fixes

Occasionally earlier branches will be chosen, (e.g. security fixes to even older versions) but they should always be carefully discussed (and documented).
