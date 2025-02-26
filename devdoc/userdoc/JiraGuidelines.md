# JIRA Creation Guidelines

This is a guide to the type of information that is desired in JIRA tickets.

# For bugs / improvement requests on any existing features or docs: 
   - User-Interface Issues:
      - Provide a screenshot when it’s applicable.  Not just of the offending line or excerpt, but of the entire application window.
      - URL to environment.
      - URL to workunit.
   - Which browser and its version.
   - Provide a zap file if there is one.
   - Build version of the environment you're using.
   - Steps for reproducing.
      - Laid out in an easy to understand format.  
      - Either a numbered set of steps or even something simple as:
         - Open Browser > Navigate to here > then click this tab 
   - Any artifacts that are relevant to the recreation of the issue.
      - These include input files, code, pkgmap contents, etc.
   - Type of environment where the HPCC Platform is running.
      - Bare-metal environment, VM, container?
      - Details on the systems used to start these such as:
         - Operation system 
         - Minikube or Docker Desktop
         - Virtual Box
   - Steps taken to install the platform if this is related to installation and init.
   - If this is a bare-metal build, where did you get your package and what is the md5sum of this package?
   - Include stack traces if possible.
      - Core files can potentially contain PII so refrain from including these in the ticket.
   - Ensure that there's no Personally Identifiable Information in the ticket.

# For new features, the reporter should provide in detail:

   - What the feature should do?
   - Why do they need it?
   - How important is it (same for a bug)?  This will help the team prioritize the issue
   - If documentation of any kind is needed (red book, blog, devdoc, etc) either add it as part of this Jira / PR, or create a new ticket for the documentation team and put in the information the doc team requested and add the doc Jira as a “relates” to the original issue.

 * Developers are encouraged to add information to the Jira as their work progresses.  This should include things like:
   - Is this a new issue or something that has been around for a long time but just now found? (will help figure when the issue got introduced)
   - Briefly describe how was the issue resolved or any information that will be helpful if someone encounters this issue again



