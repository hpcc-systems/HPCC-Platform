package framework;

import framework.config.TestClasses;
import framework.model.TestClass;
import framework.utility.Common;
import org.testng.TestNG;

import java.util.ArrayList;
import java.util.List;

public class TestRunner {
    public static void main(String[] args) {

        Common.initializeLoggerAndDriver(args[0]);

        if (Common.driver != null) {
            TestNG testng = new TestNG();
            testng.setTestClasses(loadClasses());
            testng.run();
            Common.driver.quit();
        }
    }

    private static Class<?>[] loadClasses() {

        List<Class<?>> classes = new ArrayList<>();
        for (TestClass testClass : TestClasses.testClassesList) {
            try {
                classes.add(Class.forName(testClass.getPath()));
            } catch (Exception e) {
                Common.logError("Failure: Error in loading classes: " + e.getMessage());
            }
        }

        return classes.toArray(new Class<?>[0]);
    }
}
