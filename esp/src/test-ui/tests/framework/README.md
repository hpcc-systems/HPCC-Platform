### Project: An Automated ECL Watch Test Suite

This project's code begins with the TestRunner.java file. The main method in this class loads all the Java classes
created for writing test cases for specific web pages of the ECL Watch UI and then runs the tests in those classes
sequentially.

The names of the Java classes that the TestRunner class needs to load should be listed in the config/TestClasses.java
file. ActivitiesTest class should always be the first class to load in TestClasses.java, as it gets URLs for all other web pages.

Each Java class created to write tests for specific web pages should have at least one method annotated with @Test. The
code for each class starts to run from this method.

#### Important Note: ChromeDriver Version Compatibility

If the Chrome browser version updates in the future, it's crucial to ensure that the corresponding ChromeDriver version is also updated. Failure to do so may cause tests to fail due to compatibility issues between the browser and driver. Always verify and update ChromeDriver to the latest version whenever running tests to maintain compatibility and ensure smooth test execution.

#### CLI Arguments for TestRunner.java

While running the test suite, you can pass arguments in this way -> "-l log_level -p path".
- "log_level" is of two types "debug" and "detail"
- "debug" means generate error and exception log file with a debug log file.
- "detail" means generate error and exception log file with a detailed debug file.
- If no -l and log_level is passed in the argument, only error and exception log will be generated
- "path" is the path of the folder where the json files are
- The code will log an error if the '-p' and 'path' arguments are not provided, as the JSON folder path is required for the test suite.
- -h in the CLI arguments prints the details of parameter usage to the console

path could be something like:

for GitHub Actions -> /home/runner/HPCCSystems-regression/log/

for local machine -> C:/Users/{your_working_directory_of_json_files}/

So an example of complete CLI arguments would look like this:

-l detail -p /home/runner/HPCCSystems-regression/log/

#### Implementation Steps for URL Management

- A HashMap (urlMap) is created to store URL mappings in config/URLConfig.java file. This map will use the page name as the key and a URLMapping object as the value. The URLMapping object contains the page name, its URL, and another HashMap for nested pages and tabs.
- A static block is used to initialize the urlMap with the initial URL mapping for the Activities navigation. The URL is retrieved using a method from the Common utility class, which handles the dynamic retrieval of the IP address based on the environment whether it is local or GitHub Actions.
- For each main navigation section, a URLMapping object is created. This object includes the page name and its corresponding URL. Additionally, it contains another HashMap to store URLs for nested tabs and pages.
- Each URLMapping object is stored in the urlMap with the main navigation name as the key. This initial setup in the Activities.java class ensures that each navigation section has its base URL stored and accessible.
- For instance, for any navigation page, each page has multiple tabs, and within those tabs, there are multiple pages and tabs. This structure facilitates easy access to the URL of a particular page.
- Starting from the Activities page, for each main navigation section, the code iterates over its associated tabs. For each tab, a new URLMapping object is created and added to the HashMap within the corresponding URLMapping object of the main navigation section. This creates a tree-like structure, allowing easy access to URLs for both navigation sections and their nested tabs.
- By following these implementation steps, the URLConfig class ensures that all URLs within the application are well-organized and easily accessible through a hierarchical structure. This setup simplifies navigation and URL management within the application, making it easier to handle complex page structures and dynamic URL retrievals.


Below are the dependencies used in the project:

- https://repo1.maven.org/maven2/org/testng/testng/7.7.1/testng-7.7.1.jar
- https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.17.0/jackson-annotations-2.17.0.jar
- https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.17.0/jackson-core-2.17.0.jar
- https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.17.0/jackson-databind-2.17.0.jar
- https://repo1.maven.org/maven2/com/beust/jcommander/1.82/jcommander-1.82.jar
- https://github.com/SeleniumHQ/selenium/releases/download/selenium-4.22.0/selenium-java-4.22.0.zip
- https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.30/slf4j-api-1.7.30.jar
- https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/1.7.30/slf4j-simple-1.7.30.jar

Notes: 
1. Users need to run these tests with regression test suite only.
2. Code should be updated accordingly if selenium server jar updates.
3. ActivitiesTest class should always be the first class to load in TestClasses.java, as it gets URLs for all other pages.
4. For future testing developers, custom class names or attributes defined by UI developers can change frequently during updates or redesigns. However, standard attributes that are part of the HTML specifications (such as id, type, value, href, aria-sort, aria-disabled, etc.) are much more stable. Therefore, it is advisable to use only standard HTML attributes to access web elements. This approach ensures that test cases remain consistent and are less likely to break due to UI changes.
5. Ignore the compiler warnings/errors before the beginning of the test logs. They are because of guava-33.2.1-jre-sources.jar, it seems it is not fully compatible with JRE 21, that is installed on GH Actions. But that does not impact our code in any way, so it is better to just ignore it.