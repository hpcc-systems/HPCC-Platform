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
            getCommandLineParameters(args);
            Common.checkIfLocalConfigIsSet();            
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
    // -h in the CLI arguments prints the details of parameter usage to the console

    public static void getCommandLineParameters(String[] args) { // -l <log_level> -p <path>

        String log_level = null;
        String path = null;
        boolean help = false;
        boolean quickTest = false;
        boolean showChrome = false;

        for (int i = 0; i < args.length; i++) {
            if ("-l".equals(args[i]) && i + 1 < args.length) {
                log_level = args[++i];
            } else if ("-p".equals(args[i]) && i + 1 < args.length) {
                path = args[++i];
            } else if ("-h".equals(args[i])) {
                help = true;
            } else if ("-q".equals(args[i])) {
                quickTest = true;
            } else if ("-v".equals(args[i])) {
                showChrome = true;
            }
        }

        if (log_level != null) {
            Config.LOG_LEVEL = log_level;
            System.out.println("Log level: " + Config.LOG_LEVEL);
        }

        if (path != null) {
            Config.PATH_FOLDER_JSON = path;
            System.out.println("JSON files path: " + Config.PATH_FOLDER_JSON);
        } else {
            Common.logError("Error: JSON folder path is required. Use -p <path> to specify the path.");
        }

        if (help) {
            printParameterUsage();
        }
        if (quickTest) {
            System.out.println("Set to quick test.");
            Config.TEST_DETAIL_PAGE_FIELD_NAMES_ALL = false;
            Config.TEST_WU_DETAIL_PAGE_DESCRIPTION_ALL = false;
            Config.TEST_WU_DETAIL_PAGE_PROTECTED_ALL = false;
            Config.TEST_DETAIL_PAGE_TAB_CLICK_ALL = false;
        }
        
        if(showChrome) {
            Config.SHOW_CHROME = true;
            System.out.println("Show Chrome during test.");
        }
        
    }

    private static void printParameterUsage() {
        System.out.println("""
                Requires CLI arguments: -l <log_level> -p <path>
                
                <log_level> is of two types "debug" and "detail"
                "debug" means generate error log file with a debug log file.
                "detail" means generate error log file with a detailed debug file.
                if no -l and log level is passed in the argument, only error log will be generated
                
                <path> is the path of the folder where the json files are
                -p <path> is a mandatory argument, code logs an error if the '-p' argument is not provided, as the JSON path is required for tests.
                
                Optional CLI parameters: -q -v
                -q    means quick test, only the first workunit details would be tested
                -v    means visual test, open Chrome browser to show visual trace of testing.
            """);
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
