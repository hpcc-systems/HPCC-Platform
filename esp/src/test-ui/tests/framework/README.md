### Project: An Automated ECL Watch Test Suite

This project's code begins with the TestRunner.java file. The main method in this class loads all the Java classes
created for writing test cases for specific web pages of the ECL Watch UI and then runs the tests in those classes
sequentially.

The names of the Java classes that the TestRunner class needs to load should be listed in the config/TestClasses.java
file.

Each Java class created to write tests for specific web pages should have at least one method annotated with @Test. The
code for each class starts to run from this method.

#### Important Note: ChromeDriver Version Compatibility

If the Chrome browser version updates in the future, it's crucial to ensure that the corresponding ChromeDriver version is also updated. Failure to do so may cause tests to fail due to compatibility issues between the browser and driver. Always verify and update ChromeDriver to the latest version whenever running tests to maintain compatibility and ensure smooth test execution.

#### CLI Arguments for TestRunner.java

While running the test suite, you can pass arguments in this way -> "-l log_level -p path".
- "log_level" is of two types "debug" and "detail"
- "debug" means generate error log file with a debug log file.
- "detail" means generate error log file with a detailed debug file.
- If no -l and log_level is passed in the argument, only error log will be generated
- "path" is the path of the folder where the json files are
- The code will log an error if the '-p' and 'path' arguments are not provided, as the JSON folder path is required for the test suite.

path could be something like:

for GitHub Actions -> /home/runner/HPCCSystems-regression/log/

for local machine -> C:/Users/nisha/Documents/Internship/Work/files/

So an example of complete CLI arguments would look like this:

-l debug -p C:/Users/nisha/Documents/Internship/Work/files/

Below are the dependencies used in the project:

- https://repo1.maven.org/maven2/org/testng/testng/7.7.1/testng-7.7.1.jar
- https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.17.0/jackson-annotations-2.17.0.jar
- https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.17.0/jackson-core-2.17.0.jar
- https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.17.0/jackson-databind-2.17.0.jar
- https://repo1.maven.org/maven2/com/beust/jcommander/1.82/jcommander-1.82.jar
- https://github.com/SeleniumHQ/selenium/releases/download/selenium-4.17.0/selenium-server-4.17.0.jar

Notes: 
1. Users need to run these tests with regression test suite only.
2. Code should be updated accordingly if selenium server jar updates.
3. For future testing developers, custom class names or attributes defined by UI developers can change frequently during updates or redesigns. However, standard attributes that are part of the HTML specifications (such as id, type, value, href, aria-sort, aria-disabled, etc.) are much more stable. Therefore, it is advisable to use only standard HTML attributes to access web elements. This approach ensures that test cases remain consistent and are less likely to break due to UI changes.