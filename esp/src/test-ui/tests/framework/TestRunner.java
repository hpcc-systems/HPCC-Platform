package framework;

import framework.config.Config;
import framework.config.TestClasses;
import framework.model.TestClass;
import framework.utility.Common;
import org.testng.TestNG;

import java.util.ArrayList;
import java.util.List;


public class TestRunner {
    public static void main(String[] args) {

        try {
            getLogLevelAndJSONFolderPath(args);
            Common.initializeLoggerAndDriver();

            if (Common.driver != null) {
                TestNG testng = new TestNG();
                testng.setTestClasses(loadClasses());
                testng.run();
                Common.driver.quit();
            }

            Common.printNumOfErrorsAndExceptions();

        } catch (Exception e) {
            Common.logException("Exception occurred in TestRunner class: " + e.getMessage(), e);
        }
    }

    // Parses the command-line arguments to set the log level and JSON folder path.
    // The method checks for the presence of '-l' (log_level) and '-p' (path) arguments.
    // Logs an error if the '-p' argument is not provided, as the JSON path is required.
    // path is the path of the folder where the json files are
    // log level is of two types "debug" and "detail"
    // "debug" means generate error log file with a debug log file.
    // "detail" means generate error log file with a detailed debug file.
    // if no -l and log level is passed in the argument, only error log will be generated

    public static void getLogLevelAndJSONFolderPath(String[] args) { // -l <log_level> -p <path>

        String log_level = null;
        String path = null;

        for (int i = 0; i < args.length; i++) {
            if ("-l".equals(args[i]) && i + 1 < args.length) {
                log_level = args[++i];
            } else if ("-p".equals(args[i]) && i + 1 < args.length) {
                path = args[++i];
            }
        }

        if (log_level != null) {
            Config.LOG_LEVEL = log_level;
        }

        if (path != null) {
            Config.PATH_FOLDER_JSON = path;
        } else {
            Common.logError("Error: JSON folder path is required. Use -p <path> to specify the path.");
        }
    }

    private static Class<?>[] loadClasses() {

        List<Class<?>> classes = new ArrayList<>();
        for (TestClass testClass : TestClasses.testClassesList) {
            try {
                classes.add(Class.forName(testClass.getPath()));
            } catch (Exception e) {
                Common.logException("Failure: Error in loading classes: " + e.getMessage(), e);
            }
        }

        return classes.toArray(new Class<?>[0]);
    }
}
