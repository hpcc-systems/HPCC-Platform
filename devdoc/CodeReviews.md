Some general guidelines for code reviews.

Code reviews have a few different goals:
* Catch architectural or design problems
* Catch bugs early (incorrect behaviour, inefficiencies, security issues)
* Ensure the code is readable and maintainable.
* A opportunity for training/passing on information.  
  For example providing information about how the current system works, functionality that is already available or suggestions of other approaches the developer may not have thought of.

Some general comments:
- Code reviewers should be explicit and clearly describe the problem, including what change is expected if not obvious. Don’t assume the contributor has same understanding/view as reviewer.
- If a comment is not clear the contributor should ask for clarification rather than wasting time trying to second-guess the reviewer.
- Contributors should feel free to push back if they consider comments are too picky.  The reviewer can either agree, or provide reasons why they consider it to be an issue.
- Sometimes a significant design problem means the rest of the code has not been reviewed in detail.  If this is the case it should be explicitly noted.
- Repeated issue.  The reviewer is free to comment on every instance of a repeated issue, but a simple annotation should alert contributor to address appropriately eg: [Please address all instances of this issue]
- The contributor should respond to comment if it isn't obvious where/how they have been addressed (but no need to acknowledge typo/indentation/etc)
- Only the reviewer should mark issues as resolved using the Github resolve conversation button.
- Both reviewers and contributors should respond in a timely manner - don't leave it for days.  It destroys the flow of thought and conversation.
- Watch out for collapsed conversations.  If there are large numbers of comments GitHib will collapse them which can make them easy to miss.
- If there are large number of comments > 100, it may be better to close the PR and open a new one.
- Submit any changes as extra commits so that it is clear to the reviewer what has changed, and avoids them having to re-review everything.  Please do not squash them until the reviewers approve the PR.  A few exceptions to this are if the only comments that the reviewer makes are indentation/typos, if the PR is only a couple of lines, or the PR is completely rewritten in response to the review.

The "strictness" of the review should reflect the importance and location of the change.  Some examples:
* If it is closely associated with an existing file, then the indentation, style should match the surrounding code - a mixture of styles makes it much harder to read.  If it is in a new, independent source file or project this is less of an issue.
* If the code is in a core library then efficiency and edge cases will be more important.
* If it is a core part of the system then security is key.  If it is a developer tool then edge cases are less significant.

When reading comments in a review it can sometimes be hard to know why the reviewer made a comment, or what response is expected.  If there is any doubt the contributor should ask.  However to make it clearer we are aiming to always add a tag to the front of each review comment.  The tag will give an indication of why the comment is being made, its severity and what kind of response is expected.  Here is a provisional table of tags:

Tag | What | Why | Expected response
--- | ---- | --- | -----------------
design: | An architectural or design issue | The reviewer considers the PR has a significant problem which will affect its functionality or future extensibility | reviewer/developer redesign expected before any further changes
function: | Incorrect/unexpected functionality implemented | The function doesn't match the description in the jira, or doesn't solve the original problem | developer expected to address issue (or discuss)
security: | Something in the code introduces a security problem | Highly security bugs e.g. injection attacks | developer expected to discuss the issue (and then address)
bug: | A coding issue that will cause incorrect behaviour | Likely to cause confusion, invalid results or crashes.  | developer expected to address issue
efficiency: | The code works, but may have scaling or other efficiency issues. | Inefficiency can cause problem in some key functions and areas | developer addressing the problem (or discuss)
discuss: | Reviewer has thought of a potential problem, but not sure if it applies | Reviewer has a concern it may be an issue, and wants to check the developer has thought about and addressed the issue | Discussion - either in the PR or offline.
||
style: |  Reviewer points out non-conforming code style | Makes the code hard to read | Developer to fix
indent: | A fairly obvious indentation issue | Makes the code hard to read | Developer to fix.
format: | Any other unusual formatting | Makes the code hard to read | Developer to fix.
typo: | Minor typing error | Makes something (code/message/comment) harder to read | Developer to fix.
minor: | A minor issue that could be improved. | Education (the suggestion is better for a particular reasons), or something simple to clean up at the same time as other changes | Developer recommended to fix, but unlikely to stop a merge
picky: | A very minor issue that could be improved, but is barely worth commenting on | Education, or something to clean up at the same time as other changes | Developer discretion to fix, wouldn't stop a merge
||
future: | An additional feature or functionality that fits in but should be done as a separate PR. | Ensure that missing functionality is tracked, but PRs are not held up by additional requirements. | Jira created (unless trivial) and number noted in response.
question: | Review has a question that they are not sure of the answer to | Reviewer would like clarification to help understand the code or design.  The answer may lead to further comments. | An answer to the question.
note: | Reviewer wants to pass on some information to the contributor which they may not know | Passing on knowledge/background | contributor should consider the note, but no change expected/required
personal: | Reviewer has an observation based on personal preference/experience | Similar to a note, but more of an opinion than fact.  | Reflect on the suggestion, but no change expected.
documentation: | This change may have an impact on documentation | Make sure changes can be used | Jira created describing the impact created, and number noted in response.

Whatever the aim of a comment, the comments should always be constructive.  The reviewer should have a reason for each of them, and be able to articulate the reason in the comment or when asked.  "I wouldn't have done it like that" is not a good enough on its own!

Similarly there is a difference in opinion within the team on some style issues - e.g. standard libraries or jlib, inline or out of line functions, nested or non-nested classes.  Reviews should try and avoid commenting on these unless there is a clear reason why they are significant (functionality, efficiency, compile time) and if so spell it out.  Code reviewers should discuss any style issues that they consider should be universally adopted that are not in the style guide.

