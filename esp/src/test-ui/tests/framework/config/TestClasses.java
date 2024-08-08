package framework.config;

import framework.model.TestClass;

import java.util.List;

public class TestClasses {

    // ActivitiesTest class should always be the first class to load, as it gets URLs for all other pages.

    public static final List<TestClass> testClassesList = List.of(
            new TestClass("ActivitiesTest", "framework.pages.ActivitiesTest"),
            new TestClass("ECLWorkUnitsTest", "framework.pages.ECLWorkUnitsTest")
            // The test class for FilesLogicalFilesTest is commented out because of an existing bug.
            // In ECL Watch UI on Logical Files tab, all items do not render on UI even after
            // selecting a higher dropdown, and it shows
            // empty rows. Because of this issue code is unable to get all the file items from UI
            // to go ahead for further testing. Until that issue is fixed, tests cannot check the
            // validity of the content. For testing this class, wait for the JIRA to be resolved: HPCC-32297
            //new TestClass("FilesLogicalFilesTest", "framework.pages.FilesLogicalFilesTest")
    );
}
