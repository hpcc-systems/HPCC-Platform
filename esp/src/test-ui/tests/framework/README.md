Project: An Automated ECL Watch Test Suite

This project's code begins with the TestRunner.java file. The main method in this class loads all the Java classes
created for writing test cases for specific web pages of the ECL Watch UI and then runs the tests in those classes
sequentially.

The names of the Java classes that the TestRunner class needs to load should be listed in the config/TestClasses.java
file.

Each Java class created to write tests for specific web pages should have at least one method annotated with @Test. The
code for each class starts to run from this method.