package framework.config;

import framework.model.TestClass;

import java.util.List;

public class TestClasses {

    // ActivitiesTest class should always be the first class to load, as it gets URLs for all other pages.

    public static final List<TestClass> testClassesList = List.of(
            new TestClass("ActivitiesTest", "framework.pages.ActivitiesTest"),
            new TestClass("ECLWorkUnitsTest", "framework.pages.ECLWorkUnitsTest")
    );
}
