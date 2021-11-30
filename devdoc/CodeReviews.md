# Code Review Guidelines

This document describes some of the goals and expectations for code reviewers.

## Pull requests
There are a few things that should be considered when creating a PR to increase the likelihood that they can be accepted quickly.

* Think about the checkboxes.\
  The check boxes are there to remind you to consider different aspects.  If you tick a box and have not really thought about the item then prepare to be embarrassed!
* Ensure the reviewer has enough information to review the change.\
  The code reviewer only has the JIRA and the PR to go on.  The JIRA (or associated documentation) should contain enough details to review the PR - e.g. the purpose/main aim etc.. If the scope of the jira has changed then the jira should be updated to reflect that.
* Check for silly mistakes\
  Scan the code after creating the PR to check there are no tracing/other debugging artifacts or typos.  It doesn't take long, and often catches trivial issues.  It can avoid the need for a cycle of code-review/fixes.
* Request one or more reviews (see below)

## GitHub "Reviewers" feature
Contributors should use the github reviewers section on the PR to request reviews and feedback from 1 or more user.  After a contributor has pushed a set of changes in response to a review, they should refresh the github review status, so the users are notified it is ready for re-review.
Reviewers should check for PRs that ready for their review via github's webpage (filter "review-requested:\<reviewer-id>") or via the github CLI (e.g. gh pr status)

## Review Goals
Code reviews have a few different goals:
* Catch architectural or design problems.\
  These should have been caught earlier, but better later than never...
* Catch bugs early (incorrect behaviour, inefficiencies, security issues)
* Ensure the code is readable and maintainable.\
  This includes following the project coding standards (see [Style Guide](https://github.com/hpcc-systems/HPCC-Platform/blob/master/devdoc/StyleGuide.rst)).
* A opportunity for training/passing on information.\
  For example providing information about how the current system works, functionality that is already available or suggestions of other approaches the developer may not have thought of.

It is **NOT** a goal to change the submission until it matches how the reviewer would have coded it.

## General comments
Some general comments on code reviews:
- Code reviewers should be explicit and clearly describe the problem.\
  This should include what change is expected if not obvious. Don’t assume the contributor has same understanding/view as reviewer.
- If a comment is not clear the contributor should ask for clarification.\
  ...rather than wasting time trying to second-guess the reviewer.
- Contributors should feel free to push back if they consider comments are too picky.\
  The reviewer can either agree, or provide reasons why they consider it to be an issue.
- Clearly indicate if a review is incomplete.\
  Sometimes a significant design problem means the rest of the code has not been reviewed in detail.  Other times an initial review has picked up a set of issues, but the reviewer needs to go back and check other aspects in detail.  If this is the case it should be explicitly noted.
- Repeated issues.\
  The reviewer is free to comment on every instance of a repeated issue, but a simple annotation should alert contributor to address appropriately eg: [Please address all instances of this issue]
- Contributers should provide feedback to the reviewer.\
  The contributor should respond to a comment if it isn't obvious where/how they have been addressed (but no need to acknowledge typo/indentation/etc)
- Only the reviewer should mark issues as resolved using the Github resolve conversation button.
- Code reviews should be a priority.\
  Both reviewers and contributors should respond in a timely manner - don't leave it for days.  It destroys the flow of thought and conversation.
- Check all review comments have been addressed.\
  If they have not been addressed you are guaranteed another review/submit cycle.  In particular watch out for collapsed conversations.  If there are large numbers of comments GitHub will collapse them, which can make comments easy to miss.
- Sometimes PRs need to be restarted.\
  If there are large number of comments > 100, it can be hard to track all the comments and GitHub can become unresponsive.  It may be better to close the PR and open a new one.
- Submit any changes as extra commits\.
  This makes it clear to the reviewer what has changed, and avoids them having to re-review everything.  Please do not squash them until the reviewers approve the PR.  The few exceptions to this are if the PR is only a couple of lines, or the PR is completely rewritten in response to the review.
- Reviewers use GitHub's features\
  Making use of the "viewed" button can make it easier to track what has changed - or quickly remove trivial changes from view.  Ignoring whitespace can often simplify comparisons - especially when code has been refactored or extra conditions or try/catch bocks have been introduced.

## Strictness
All code reviews don't need to be equally strict.  The "strictness" of the review should reflect the importance and location of the change.  Some examples:
* If it is closely associated with an existing file, then the indentation, style should match the surrounding code - a mixture of styles makes it much harder to read.  If it is in a new, independent source file or project this is less of an issue.
* If the code is in a core library then efficiency and edge cases will be more important.
* If it is a core part of the system then security is key.  If it is a developer only tool then edge cases are less significant.
* Reviews of draft pull requests are likely to concentrate on the overall approach, rather than the details.  They are likely to be more informal (e.g. not always using comments tags).

## Comment tags
When reading comments in a review it can sometimes be hard to know why the reviewer made a comment, or what response is expected.  If there is any doubt the contributor should ask.  However to make it clearer we are aiming to always add a tag to the front of each review comment.  The tag will give an indication of why the comment is being made, its severity and what kind of response is expected.  Here is a provisional table of tags:

Tag | What | Why | Expected response
--- | ---- | --- | -----------------
design: | An architectural or design issue | The reviewer considers the PR has a significant problem which will affect its functionality or future extensibility | reviewer/developer redesign expected before any further changes
scope: | The scope of the PR does not match the Jira | If the scope of the fix is too large it can be hard to review, and take much longer to resolve all the issues before the PR is accepted. | Discussion.  Split the PR into multiple simpler PRs.
function: | Incorrect/unexpected functionality implemented | The function doesn't match the description in the jira, or doesn't solve the original problem | developer expected to address issue (or discuss)
security: | Something in the code introduces a security problem | The reviewer has spotted potential security issues e.g. injection attacks | developer expected to discuss the issue (and then address)
bug: | A coding issue that will cause incorrect behaviour | Likely to cause confusion, invalid results or crashes.  | developer expected to address issue
efficiency: | The code works, but may have scaling or other efficiency issues. | Inefficiency can cause problem in some key functions and areas | developer addressing the problem (or discuss)
discuss: | Reviewer has thought of a potential problem, but not sure if it applies | Reviewer has a concern it may be an issue, and wants to check the developer has thought about and addressed the issue | Discussion - either in the PR or offline.
||
style: |  Reviewer points out non-conforming code style | Makes the code hard to read | Developer to fix
indent: | A fairly obvious indentation issue | Makes the code hard to read | Developer to fix.
format: | Any other unusual formatting | Makes the code hard to read | Developer to fix.
typo: | Minor typing error | Makes something (code/message/comment) harder to read | Developer to fix.
minor: | A minor issue that could be improved. | Education (the suggestion is better for a particular reason), or something simple to clean up at the same time as other changes | Developer recommended to fix, but unlikely to stop a merge
picky: | A very minor issue that could be improved, but is barely worth commenting on | Education, or something to clean up at the same time as other changes | Developer discretion to fix, wouldn't stop a merge
||
future: | An additional feature or functionality that fits in but should be done as a separate PR. | Ensure that missing functionality is tracked, but PRs are not held up by additional requirements. | Contributor to create Jira (unless trivial) and number noted in response.
question: | Review has a question that they are not sure of the answer to | Reviewer would like clarification to help understand the code or design.  The answer may lead to further comments. | An answer to the question.
note: | Reviewer wants to pass on some information to the contributor which they may not know | Passing on knowledge/background | contributor should consider the note, but no change expected/required
personal: | Reviewer has an observation based on personal experience | Reviewer has comments that would improve the code, but not part of the style guide or required.  E.g. patterns for guard conditions  | Reflect on the suggestion, but no change expected.
documentation: | This change may have an impact on documentation | Make sure changes can be used | Contributor to create Jira describing the impact created, and number noted in response.

The comments should always be constructive.  The reviewer should have a reason for each of them, and be able to articulate the reason in the comment or when asked.  "I wouldn't have done it like that" is not a good enough on its own!

Similarly there is a difference in opinion within the team on some style issues - e.g. standard libraries or jlib, inline or out of line functions, nested or non-nested classes.  Reviews should try and avoid commenting on these unless there is a clear reason why they are significant (functionality, efficiency, compile time) and if so spell it out.  Code reviewers should discuss any style issues that they consider should be universally adopted that are not in the [style guide](https://github.com/hpcc-systems/HPCC-Platform/blob/master/devdoc/StyleGuide.rst).

