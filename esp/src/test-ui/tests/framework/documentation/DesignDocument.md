**The Class Design and Code Flow for ECL Watch Test Suite**

**1. TestRunner.java**

![img.png](TestRunner.png)

The TestRunner class is responsible for setting up and executing automated tests using the TestNG framework in a
Selenium-based testing environment. It handles the initialization of logging and WebDriver, dynamically loads the 
test classes to be executed, and runs the tests.

Variables:
args: Command line arguments passed to the main method. args[0] defines type of logging (it could be "debug" or "detail") used for 
initializing the logger. "debug" argument creates a debug log file with the debug logs and "detail" argument creates a fine 
detailed debug log file.

**Methods:**

1. main Method

The entry point of the application. It performs the following steps:

- Calls Common.initializeLoggerAndDriver to configure the logging system and web driver, which ogs test execution details to a specified file.
- If the drives sets up properly and is not null, it creates an instance of TestNG.
- Sets the test classes to be run by calling loadClasses.
- Runs the tests using TestNG.
- Quits the WebDriver session after the tests have completed.

2. loadClasses Method

Loads the test classes specified in TestClasses.testClassesList:

- Iterates over the testClassesList and loads each class by its fully qualified name.
- Adds the loaded class to the list classes.
- Catches and prints any ClassNotFoundException.

**2. TestClasses.java**

![img_4.png](TestClasses.png)

The TestClasses class in the framework.config package is responsible for maintaining
a list of test classes used in the framework. This class provides a centralized and
immutable collection of test class metadata, which includes the name of each test
class and its fully qualified class name. This setup helps organize and reference
the test classes easily throughout the testing framework. By using the TestClasses
class, the framework can dynamically load and execute tests, enhancing modularity
and maintainability.

**3. TestClass.java**

![img_3.png](TestClass.png)

The TestClass class in the framework.model package is a simple model class designed
to encapsulate metadata about a test class within the testing framework. It contains
the name of the test class and its fully qualified class name (path). This class
provides a structured way to store and retrieve information about each test class,
which can be utilized by other components in the framework for dynamically loading and
executing tests.

**4. Config.java**

![img.png](Config.png)

The Config class in the framework.config package serves as a centralized configuration
repository for the application. It contains various constants that are used throughout
the framework, providing a single point of reference for configuration settings such as
file paths, IPs, and other constants. This approach enhances the maintainability and
readability of the code by avoiding hard-coded values scattered across different classes.

**5. Common.java**

![img.png](Common.png)

The Common class in the framework.utility package provides a set of utility
methods that are frequently used throughout the testing framework. These
methods handle common tasks and leverages constants from the Config class to maintain consistency 
and facilitate configuration management.

Variables:

driver: This public static variable stores the WebDriver instance used for interacting with the web browser.
errorLogger: This public static variable stores a logger instance used for logging error messages.
specificLogger: This public static variable stores a logger instance used for logging specific messages 
based on the provided level (debug or detail).

**Methods:**

1. checkTextPresent Method

Checks if the specified text is present on the current webpage and logs the result.

- Retrieves the page source using driver.getPageSource().
- Checks if the page source contains the specified text.
- Logs a success message if the text is found; otherwise, logs an error message and records it using the provided
  logger.

2. openWebPage Method

Opens the specified URL in the browser and maximizes the window.

- Navigates to the specified URL using driver.get(url).
- Maximizes the browser window using driver.manage().window().maximize().
- Calls the sleep method to pause the execution for a short period to allow the page to load completely.

3. sleep Method

The sleep method pauses the execution of the program for a specified duration
(4 seconds in this case). This can be useful in scenarios where a delay is required,
such as waiting for a webpage to load completely before proceeding with further actions.

4. isRunningOnLocal Method

Determines if the code is running on a local environment.

- Checks if the operating system name starts with the value of Config.LOCAL_OS.
- Checks if the user profile path starts with the value of Config.LOCAL_USER_PROFILE.
- Returns true if both conditions are met, indicating a local environment; otherwise, returns false.

5. getUrl Method

Constructs the full URL based on the environment (local or GitHub Actions).

- Calls isRunningOnLocal to check the environment.
- If running locally, returns the URL prefixed with Config.LOCAL_IP.
- If running in GitHub Actions, returns the URL prefixed with Config.GITHUB_ACTION_IP.

6. waitForElement Method

- This method waits for a web element to be present in the DOM.
- Uses WebDriverWait to wait up to 10 seconds for the presence of the specified web element.

7. logError Method

- This method logs error messages.
- Prints the error message to the standard error stream.
- Logs the message using errorLogger.

8. logDebug Method

- This method logs debug or detailed messages.
- Prints the message to the standard output stream.
- Logs the message using specificLogger if the logging level is INFO or FINE.

9. logDetail method

- This method logs detailed messages.
- Prints the message to the standard output stream.
- Logs the message using specificLogger if the logging level is FINE.

10. initializeLoggerAndDriver Method

- This method initializes the logger and WebDriver instances.
- Sets up the errorLogger and specificLogger based on the provided argument.
- Sets up the WebDriver instance using setupWebDriver() method.

11. setupWebDriver Method

- This method sets up the WebDriver instance based on the environment.
- Configures ChromeOptions for headless mode, no sandbox, and suppressed log output.
- Sets up the WebDriver based on the environment (local or GitHub Actions).
- Logs an error message if an exception occurs during setup.

12. setupLogger Method

- This method sets up a logger instance based on the provided log level.
- Configures the logger to disable console logging and set up file handlers for different log levels (error, debug, detail).
- Turns off all logging from Selenium WebDriver. 
- Logs an error message if an exception occurs during logger setup.

13. getAttributeList Method

- This method retrieves all attributes of a web element.
- Uses JavaScriptExecutor to extract all attributes of the web element.
- Logs an error message if an exception occurs during attribute extraction.

**6. TimeUtils.java**

![img.png](TimeUtils.png)

The TimeUtils class in the framework.utility package provides utility functions to handle
time strings and convert them into milliseconds. This is useful for standardizing time
representations and performing time-based calculations in a consistent manner.

TIME_PATTERN: A regular expression pattern used to match various time formats. The supported formats are:

- d days h:m:s.s
- h:m:s.s
- m:s.s
- s.s

**Methods:**

1. convertToMilliseconds Method

This method converts a time string into milliseconds based on the matched pattern. If the
time string does not match any recognized format, it returns a predefined constant for
malformed time strings.

- The method first attempts to match the input time string against the TIME_PATTERN.
- If the string matches the pattern, it initializes the time components (days, hours, minutes, seconds, milliseconds) to
  zero.
- The method then extracts values based on the matching groups.
- If any parsing errors occur (e.g., NumberFormatException) or if the string does not match the pattern, the method
  returns Config.MALFORMED_TIME_STRING.
- If the parsing is successful, the method calculates the total duration in milliseconds

**7. NavigationWebElement**

![img.png](NavigationWebElement.png)

This NavigationWebElement class in the framework.model package, is a record class used to represent a navigation element
within a web application framework. It offers a concise way to store and manage information about such
elements.

- The class is defined as a record which is a recent addition to Java that simplifies creating immutable data classes.
- It has three properties:
    - name: A String representing the name or identifier of the navigation element in the menu bar(e.g., "Activities", "
      ECL", "Files).
    - hrefValue: A String representing the href attribute value of the element, which typically specifies the URL linked
      to by the element.
    - webElement: A WebElement object from the Selenium library. This holds the actual WebElement instance representing
      the element in the web page.

- Due to the record nature, a constructor is not explicitly defined. The compiler generates a constructor that takes
  arguments for each property and initializes them.
- The NavigationWebElement class offers a structured way to manage data related to navigation elements in a web
  application framework.

**8. Java Classes for Representing Workunit JSON Data**

This section details the class structure used to map JSON data file of list of "Workunit"
entities into Java objects. These classes provide a clear representation of the data and
allow for easy access to its values throughout the codebase. This structure is particularly
beneficial for writing test cases, as it simplifies working with the data in a well-defined
format. Below are the UML diagram of the classes used for JSON mapping to java objects for
workunits JSON file.

![img.png](WUQueryRoot.png)
![img.png](WUQueryResponse.png)
![img.png](Workunits.png)
![img.png](ECLWorkunit.png)
![img.png](ApplicationValues.png)
![img.png](ApplicationValue.png)

**9. ActivitiesTest**

![img.png](ActivitiesTest.png)

This ActivitiesTest class in the framework.pages package, implements a TestNG test (@Test)
for the Activities page of ECL Watch UI. It focuses on verifying the following aspects of the
Activities page:

- Presence of specific text elements
- Functionality of navigation links and their corresponding sub-tabs

**Class Variables:**

- textArray: A static final String array containing expected text elements to be present on the Activities page (e.g., "
  Target/Wuid", "Graph").
- navNamesArray: A static final String array containing names used to locate the navigation link WebElements on the
  Activities page (e.g., "Activities", "ECL", "Files).
- tabsListMap: A static final Map that defines the expected sub-tabs for each navigation link. The key is the navigation
  link name, and the value is a List of expected sub-tabs.

**Methods:**

1. testActivitiesPage (Test Method)

- This is the main test method annotated with @Test to be run as a test case.
- Initializes the WebDriver instance. 
- Opens the "Activities" page using the URL from the configuration. 
- Logs the start of the tests for the "Activities" page. 
- Calls testForAllText(driver) to check for the presence of predefined texts. 
- Retrieves the navigation web elements by calling getNavWebElements(driver). 
- Calls testForNavigationLinks(driver, navWebElements) to test the navigation links. 
- Logs the completion of the tests for the "Activities" page.

2. testForAllText Method

- This method checks if specific texts are present on the "Activities" page.
- Logs the start of text presence tests. 
- Iterates over each text in textArray. 
- Calls Common.checkTextPresent(driver, text, "Activities Page") to verify the presence of each text on the page.

3. testForNavigationLinks Method

- This method tests each navigation link to ensure they direct to the correct pages with the expected tabs. 
- Logs the start of navigation link tests. 
- Iterates over each NavigationWebElement in navWebElements. 
- Clicks on each navigation element and verifies the presence of corresponding tabs by calling testTabsForNavigationLinks(driver, element). 
- Logs success if all tabs are present; otherwise, logs an error with the current page details. 
- Catches and logs any exceptions that occur during the process.

4. getCurrentPage Method

- This method determines the current page by checking the presence of specific tabs.
- Iterates over each entry in tabsListMap. 
- Checks if all tabs for each page are present in the page source. 
- Returns the page name if all tabs are present; otherwise, returns "Invalid Page".

5. testTabsForNavigationLinks Method:

- This method verifies if all tabs corresponding to a navigation element are present on the current page.
- Retrieves the expected sub-tabs list for the navigation element from tabsListMap.
- Gets the current page source using driver.getPageSource().
  - Iterates through the expected sub-tabs list:
      - Checks if each sub-tab is present in the page source.
      - If any sub-tab is missing, returns false.
- If all sub-tabs are found, returns true.

6. getNavWebElements Method:

- Creates an empty list to store NavigationWebElement objects.
- Iterates through the navNamesArray.
- For each navigation link name:
    - Finds the WebElement using driver.findElement with By.name strategy.
    - Extracts the href attribute value.
    - Creates a new NavigationWebElement object with the name, href value, and WebElement reference.
    - Adds the NavigationWebElement to the list.
- Returns the list of NavigationWebElement objects.

**10. BaseTableTest**

![img.png](BaseTableTest.png)

This abstract class, `BaseTableTest`, in the framework.pages package, provides a framework for
testing web pages that display tabular data. It defines methods for common functionalities like:

- Verifying the presence of expected text elements on the page.
- Comparing the content displayed in the table with corresponding data from a JSON file.
- Testing the sorting functionality of the table columns.
- Verifying links within the table cells and their navigation behavior.

**Abstract Methods:**

1. `getPageName`: Returns the name of the page under test.
2. `getPageUrl`: Returns the URL of the page under test.
3. `getJsonFilePath`: Returns the file path of the JSON file containing reference data.
4. `getColumnNames`: Returns an array of column names displayed in the table header.
5. `getColumnKeys`: Returns an array of unique keys used to identify table headers.
6. `getUniqueKeyName`: Returns the name of the unique key used to identify table rows.
7. `getUniqueKey`: Returns the unique key value for the current row (used for logging).
8. `getColumnKeysWithLinks`: Returns an array of column keys that contain links within the table cells.
9. `parseDataUIValue`: Parses and pre-processes a data value extracted from the UI table.
10. `parseDataJSONValue`: Parses and pre-processes a data value extracted from the JSON file.
11. `parseJson`: Parses the JSON file and returns a list of objects representing the data.
12. `getColumnDataFromJson`: Extracts a specific data value from a JSON object based on the provided column key.
13. `sortJsonUsingSortOrder`: Sorts the list of JSON objects based on a given column key and sort order.
14. `getCurrentPage`: Retrieves the name of the current page displayed in the browser.

**Non-Abstract Methods:**

1. `testPage`: The main test method that orchestrates all other functionalities to test the target web page.

- Opens the target webpage using the URL obtained from `getPageUrl`.
- Calls `testForAllText` to verify the presence of expected text elements.
- Calls `testContentAndSortingOrder` to compare table data and test sorting functionality.
- Calls `testLinksInTable` to verify links within table cells and their navigation behavior.

2. `testLinksInTable`: This method tests the links in the table to ensure they direct to the correct pages.

- Logs the start of link tests for the page. 
- Iterates over each column key that should contain links. 
- Retrieves values from the UI for the current column. 
- Iterates over each value, clicks the corresponding link, and verifies the navigation. 
- Logs success if the navigation is correct; otherwise, logs an error. 
- Refreshes the page and verifies if dropdown values remain unchanged.

3. `testContentAndSortingOrder`:

- Retrieves all objects from the JSON file using `getAllObjectsFromJson`.
- Clicks on the dropdown menu to select an appropriate number of items to display in the table.
- Calls `testTableContent` to compare the table data with JSON data.
- Iterates through each column:
    - Calls `testTheSortingOrderForOneColumn` to test sorting functionality for the current column.

4. `testTheSortingOrderForOneColumn`:

- Gets the current sorting order for the specified column using `getCurrentSortingOrder`.
- Extracts data from the UI table and JSON file for the specified column.
- Sorts the JSON objects based on the current sorting order using `sortJsonUsingSortOrder`.
- Compares the sorted JSON data with the extracted UI data using `compareData`.
- Logs success if the data is sorted correctly; otherwise, logs an error.

5. `getCurrentSortingOrder`: 

- This method retrieves the current sorting order for a column.
- Finds the column header and clicks it. 
- Retrieves and returns the sort order attribute from the column header. 
- Logs and returns null if an error occurs.

6. `getDataFromJSONUsingColumnKey`: Extracts a list of data values for a specified column from all JSON objects.

7. `getDataFromUIUsingColumnKey`: 

- Extracts a list of data values for a specified column from all table cells (UI).
- Finds elements in the UI corresponding to the column key. 
- Retrieves and returns the text content of the elements. 
- Logs and returns an empty list if an error occurs.

8. `ascendingSortJson`: Sorts a list of JSON objects in ascending order based on a specified column key using a comparator.

9. `descendingSortJson`: Sorts a list of JSON objects in descending order based on a specified column key using a comparator.

10. `testTableContent`:

- This method tests if the table content matches the JSON data.
- Logs the number of objects from JSON. 
- Retrieves data from the UI for the unique key. 
- Compares the number of objects from JSON with the number of objects from the UI. 
- Iterates over each column, retrieves data from the UI and JSON, and compares them. 
- Logs and returns the result of the comparison.

11. `getAllObjectsFromJson`: Parses the JSON file and returns a list of objects representing the data. Handles potential
    exceptions during parsing.

12. `compareData`: Compares a list of data values extracted from the UI table with the corresponding list from the JSON
    file.

- Iterates through each data pair:
    - Calls `parseDataUIValue` and `parseDataJSONValue` to pre-process the data (if needed).
    - Calls `checkValues` to perform the actual comparison and handle mismatches.

13. `checkValues`: 

- Compares two data values and logs an error message if they don't match.
- Compares the values from the UI and JSON. 
- Logs an error if the values are not equal. 
- Returns the result of the comparison.

14. `clickDropdown`:

- This method selects a dropdown value to ensure all items are displayed.
- Finds the dropdown element and clicks it. 
- Waits for the dropdown options to be visible. 
- Selects the smallest dropdown value greater than the number of JSON items. 
- Refreshes the page and logs an error if an exception occurs.

15. `getSelectedDropdownValue`:

- This method retrieves the selected value from the dropdown.
- Waits for the dropdown element to be visible and returns its text content. 
- Logs and returns an empty string if an error occurs.

16. `testForAllText`:

- This method tests if specific texts are present on the page.
- Logs the start of text presence tests. 
- Iterates over each column name and verifies its presence on the page.

**11. ECLWorkUnitsTest**

![img.png](ECLWorkUnitsTest.png)

This class, `ECLWorkUnitsTest`, in the framework.pages package, extends the `BaseTableTest`
class and specifically implements test cases for the ECL Workunits page within the ECL Watch UI.
It inherits functionalities for common table testing procedures and specializes them for the
ECL Workunits data and behavior.

**Override Methods:**

1. `getPageName`: Returns the display name of the page under test ("ECL Workunits").
2. `getPageUrl`: Returns the URL for the ECL Workunits page constructed using `Config.ECL_WORK_UNITS_URL`.
3. `getJsonFilePath`: Determines the file path of the JSON file containing ECL Workunit data based on the test execution
   environment (local vs. GitHub Actions).
4. `getColumnNames`: Returns an array of column names displayed in the ECL Workunits table header.
5. `getColumnKeys`: Returns an array of unique keys used to identify ECL Workunit table headers.
6. `getColumnKeysWithLinks`: Returns an array specifying the column key that contains links within table cells (
   currently, only "Wuid" has links).
7. `getUniqueKeyName`: Returns the name of the unique key used to identify ECL Workunit table rows ("WUID").
8. `getUniqueKey`: Returns the actual value of the unique key for the current row (used for logging).
9. `parseJson`: Parses the JSON file specific to ECL Workunits and returns a list of `ECLWorkunit` objects representing
   the data.
10. `getColumnDataFromJson`: Extracts a specific data value from an `ECLWorkunit` object based on the provided column
    key.
11. `parseDataUIValue`: Parses and pre-processes a data value extracted from the ECL Workunits UI table.
12. `parseDataJSONValue`: Parses and pre-processes a data value extracted from the ECL Workunits JSON file (if
    necessary).
13. `sortJsonUsingSortOrder`: Sorts the list of `ECLWorkunit` objects based on a given column key and sort order,
    considering the specific sorting behavior for the ECL Workunits table (default sort is descending by WUID).
14. `getCurrentPage`: Retrieves the title attribute of the "wuid" element to determine the current page displayed.

**Additional Methods:**

1. `getCostColumns`: Returns a list of column names that represent cost-related data (Compile Cost, Execution Cost, File
   Access Cost).

**Test Method:**

1. `testingECLWorkUnitsPage`: Calls the inherited `testPage` method to execute the core test logic for the ECL Workunits
   page.

**Class Relationship Structure:**

![img.png](relationship.png)





