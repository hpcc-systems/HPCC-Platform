package framework.config;

import framework.model.TestClass;

import java.util.List;

public class TestClasses {

    public static final List<TestClass> testClassesList = List.of(
            //new TestClass("ActivitiesTest", "framework.pages.ActivitiesTest"),
            new TestClass("ECLWorkUnitsTest", "framework.pages.ECLWorkUnitsTest")
    );
}
