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

## Target branch
The [Version support](VersionSupport.md) document contains details of the different versions that are supported, and which version should be targetted for different kinds of changes.  Occasionally earlier branches will be chosen, (e.g. security fixes to even older versions) but they should always be carefully discussed (and documented).

Changes will always be upmerged into the next point release for all the more recent major and minor versions (and master).
