# Contributing Documentation to the HPCC Systems Platform Project

This document is intended for anyone that wants to contribute
documentation to our project. The first audience is platform developers,
so we can streamline the process of documenting new features. However,
these guidelines apply to anyone who wants to contribute to any of our
documentation (Language Reference, Programmer’s Guide, etc.).

This set of guidelines should help you understand the information needed
to create sufficient documentation for any new feature you add. The good
news is that we are all here to help you and support your writing
efforts. We will help by advising you along the way, and by reviewing
and editing your submissions.

## Documenting a New Software Feature--Required and Optional Components

When you create a new feature or function, clear documentation is
crucial for both internal teams and external users. You worked hard on
the feature, so it deserves proper notice and usage.

Contributions to the platform are always welcome, and we strongly encourage 
developers and users to contribute documentation.

You can contribute on many levels:

1.  Developer Notes

2.  End user “Readmes” in the form of MD files in the GitHub repository

3.  Blogs

4.  Formal documentation

Regardless of the form you are planning to deliver, here are the
required and optional components to include in a document.

> **Tip**: VS Code is very good at editing MD files. There is a built-in Preview
> panel available to be able to see the rendered form.
>
> In addition, GitHub Copilot is MD-aware and can help you write and
> format. For example, you can ask the Copilot, “How can I align the
> content within the cells of my Markdown table?” GitHub copilot will
> show you the alignment options.

### Required Components:

1.  **Overview**

    -   **What it is:** Briefly describe the feature's purpose and the
        problem it solves.

    -   **Why it matters:** Explain the value proposition for users and
        the overall impact on the software.

    -   **Target audience:** Specify who this feature is designed for
        (for example, all users or specific user roles).

    -   **Use Cases:** Provide concrete examples of how a user might
        leverage this feature in a real-world scenario.

2.  **Installation and Configuration:** Details on how to install and
    basic setup for use, if needed. This must include any system
    requirements or dependencies.

3.  **User Guide / Functionality**

    -   **How it works:** Provide a task-oriented, step-by-step guide
        for using the feature. If possible, include screenshots for visual
        learners.

    -   **Tips, Tricks, and Techniques:** Explain any shortcuts or
        clever uses of the feature that may be non-obvious.

    -   **Inputs and Outputs:** Detail the information users need to
        provide to the feature and the format of the results.

    -   **Error Handling:** Explain what happens if users encounter
        errors and how to troubleshoot common issues.

4.  **Limitations and Considerations:**

    -   **Limitations:** Acknowledge any restrictions or boundaries
        associated with the feature's functionality.

### Optional Components:

1.  **Advanced Usage**

    -   **Detailed configuration options:** If the feature offers
        advanced settings or customizations, provide in-depth
        instructions for experienced users.
        This is similar to the way some options command line program's usage 
        are only displayed using the verbose option. 

2.  **API Reference (for technical audiences)**

    -   **Technical specifications:** For features with an API component, 
        include detailed API reference documentation for developers integrating 
        it into their applications.

3.  **FAQs**

    -   **Frequently Asked Questions:** Address any commonly anticipated
        user questions about the feature to pre-empt confusion.

4.  **Additional Resources**

    -   **Links to related documentation:** Include links to relevant
        documentation for features that interact with this new addition.

    -   **Tutorials:** Consider creating tutorials for a more
        interactive learning experience.

    -   **Videos:** Consider requesting that a video be made to provide
        a more interactive visual learning experience. You should
        provide a simple script or outline of what should be shown in
        the video.

### General Tips

-   **Target your audience:** Tailor the level of detail and technical
    jargon you use based on whether the documentation is for developers
    or end-users.  

-   **Clarity and Conciseness:** Use clear, concise language and
    maintain a consistent structure for easy navigation. Always use
    present tense, active voice. Remember, you’re writing for users and 
    programmers, not academics, so keep it simple and straightforward. 
    See the [HPCC Style Guide](HPCCStyleGuide) for additional guidance.

-   **Visual Aids:** Screenshots, diagrams, and flowcharts can
    significantly enhance understanding. A picture can communicate
    instantly what a thousand words cannot.

-   **Maintain and Update:** Regularly review and update documentation
    as the feature evolves or based on user feedback.

By following these guidelines and including the required and optional
components, you can create comprehensive documentation that empowers
users and streamlines the adoption of your new software feature.

### Who should write it?

The boundary between a developer's responsibilities and the documentation 
team’s responsibility is not cast in stone. However, there are some guidelines 
that can help you decide what your responsibility is. Here are some examples:

#### Changing the default value of a configuration setting

This typically needs a simple one or two word change in the area of the 
documentation where that setting is documented. However, the change could impact 
existing deployments or existing code and therefore it might also require a short 
write-up for the Red Book and/or Release Announcement.
If the setting is used by both bare-metal and containerized, you should provide 
information about how the new setting is used in each of those deployments.

#### Adding or modifying a Language keyword, Standard Library function, or command line tool action

This needs some changes to existing documentation so the best way to provide the 
information is in a documentation Jira issue.
If it s a new keyword, function, or action, a brief overview should be included.
For a Standard Library function, the developer should update the Javadoc comment 
in the appropriate ECL file. For a command line tool change, the developer should 
update the Usage section of the code.

#### Adding a new feature that requires an overview.

This is a candidate for either an MD file, a blog, or both. Since there should have 
been some sort of design specification document, that could easily be repurposed as 
a good start for this. 

#### A feature/function that is only used internally to the system

Since this is information that is probably only of interest to other developers, 
a write-up in the form of an MD file in the repo is the best approach. If it 
affects end-users or operations, then a more formal document or a blog might be 
a good idea. 
If it affects existing deployments or existing code, then a Red Book notice might 
also be needed.

#### Extending the tests in the regression suite

New tests are frequently added and the regression suite readme files should be 
updated at the same time. If the tests are noteworthy, we could add a mention in 
the Platform Release Notes.  

### Placement

In general, it makes sense to keep simple documentation near the code. For
example, a document about ECL Agent should go in the ECLAgent folder.
However, there are times where that is either not possible or a document
may cover more than one component. In those cases, there are a few
options as shown below.

#### Other Folders

**devdoc**: This is a general folder for any developer document.

**devdoc**/**docs**: This is a folder for documents about documentation.

**devdoc**/**userdoc**: This is a collection of docs targeted toward the end-user
rather than developers.

This is primarily for informal documents aimed at end-users. This info can and should 
be incorporated into the formal docs. 
**devdoc**/**userdoc**/**troubleshoot**: Information related to troubleshooting
particular components

**devdoc**/**userdoc**/**azure**: Useful information about Azure Cloud portal and
cli

**devdoc**/**userdoc**/**roxie**: Useful information for running roxie

**devdoc**/**userdoc**/**thor**: Useful information for running thor

**devdoc**/**userdoc**/**blogs**: COMING SOON: Location and storage of original
text for blogs. It also has docs with guidelines and instructions on
writing Blogs

#### Pull Requests

You can include your documentation with your code in a Pull Request or
create a separate Jira and Pull Request for the documentation. This
depends on the size of the code and doc. For a large project or change, a separate 
Pull request for the documentation is better. This might allow the code change to be 
merged faster. 

#### Documentation Jira Issues

For minor code changes, for example the addition of a parameter to an existing ECL 
keyword, you can request a documentation change in a Jira issue. 
You should provide sufficient details in the Jira. 

For example, If you add an optional parameter named Foo, you should provide 
details about what values can be passed in through the Foo parameter and what 
those values mean. You should also provide the default value used if the 
parameter is omitted.