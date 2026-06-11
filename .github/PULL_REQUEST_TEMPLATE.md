<!-- Thank you for submitting a pull request to the HPCC project

 PLEASE READ the following before proceeding.

 This project expects pull requests to be linked to a relevant GitHub issue when applicable.
 If suggesting a new feature or change, please open or reference a GitHub issue first.
 If fixing a bug, there should be an issue describing it with steps to reproduce.
 The pull request title (and first commit title) should follow Conventional Commits format:

 <type>(<optional scope>)<!>: <description>

 The pull request body and first commit message should include one standalone directive line:
 Fixes #<n>, Closes #<n>, Resolves #<n>, or NoIssue

 The pull request body and first commit message should also include one or more Impacts lines:
 Impacts: <component>[, <component> ...]

 Please go over all the following points, and put an `x` in all the boxes that apply. You may find
 it easier to press the 'Create' button first then click on the checkboxes to edit the comment.
-->

## Type of change:
- [ ] This change is a bug fix (non-breaking change which fixes an issue).
- [ ] This change is a new feature (non-breaking change which adds functionality).
- [ ] This change improves the code (refactor or other change that does not change the functionality)
- [ ] This change fixes warnings (the fix does not alter the functionality or the generated code)
- [ ] This change is a breaking change (fix or feature that will cause existing behavior to change).
- [ ] This change alters the query API (existing queries will have to be recompiled)

## Checklist:
- [ ] My code follows the code style of this project.
  - [ ] My code does not create any new warnings from compiler, build system, or lint.
- [ ] The commit message is properly formatted and free of typos.
  - [ ] The commit message title makes sense in a changelog, by itself.
  - [ ] The commit is signed.
- [ ] My change requires a change to the documentation.
  - [ ] I have updated the documentation accordingly, or...
  - [ ] I have created a GitHub issue to update the documentation.
  - [ ] Any new interfaces or exported functions are appropriately commented.
- [ ] I have read the CONTRIBUTORS document.
- [ ] The change has been fully tested:
  - [ ] I have added tests to cover my changes.
  - [ ] All new and existing tests passed.
  - [ ] I have checked that this change does not introduce memory leaks.
  - [ ] I have used Valgrind or similar tools to check for potential issues.
- [ ] I have given due consideration to all of the following potential concerns:
  - [ ] Scalability
  - [ ] Performance
  - [ ] Security
  - [ ] Thread-safety
  - [ ] Cloud-compatibility
  - [ ] Premature optimization
  - [ ] Existing deployed queries will not be broken
  - [ ] This change fixes the problem, not just the symptom
  - [ ] The target branch of this pull request is appropriate for such a change.
- [ ] There are no similar instances of the same problem that should be addressed
  - [ ] I have addressed them here
  - [ ] I have raised GitHub issues to address them separately
- [ ] This is a user interface / front-end modification
  - [ ] I have tested my changes in multiple modern browsers
  - [ ] The component(s) render as expected

## Smoketest:
- [ ] Send notifications about my Pull Request position in Smoketest queue.
- [ ] Test my draft Pull Request.

## Testing:
<!-- Please describe how this change has been tested.-->

<!-- Thank you for taking the time to submit this pull request and to answer all of the above-->
