package framework.config;

public class Config {

    public static final String LOG_FILE_ERROR = "error_ecl_test.log"; // name of the error log file generated after code run is completed
    public static final String LOG_FILE_EXCEPTION = "exception_ecl_test.log"; // name of the exception log file generated after code run is completed
    public static final String LOG_FILE_DEBUG = "debug_ecl_test.log"; // name of the debug log file generated after code run is completed
    public static final String LOG_FILE_DETAIL = "detail_ecl_test.log"; // name of the detail log file generated after code run is completed
    public static final String LOCAL_OS = "Windows"; // name of your local OS, it is used to identify whether the code is running on a local machine or on GitHub Actions.
    public static final String LOCAL_USER_PROFILE = "C:\\Users\\{your_username}"; // name of your local user profile, it is used to identify whether the code is running on a local machine or on GitHub Actions.
    public static final String PATH_LOCAL_CHROME_DRIVER = "C:/Users/{your_working_directory_for_chromedriver}/chromedriver.exe"; // path of chrome driver on your local machine
    public static final String PATH_GH_ACTION_CHROME_DRIVER = "/usr/bin/chromedriver"; // path of chrome driver on GitHub Actions
    public static final int MALFORMED_TIME_STRING = -1; // this integer is used to denote any malformed time string that we can get from the UI or JSON file.
    public static final int WAIT_TIME_IN_SECONDS = 1; // this is the default wait time that code uses to load any web element on UI.
    public static final int WAIT_TIME_THRESHOLD_IN_SECONDS = 20; // This time is used to stop the code from waiting infinitely. If it is unable to find a web element on the UI, the code stops the search after this time logs an error if the element is not found.
    public static final String TEST_DESCRIPTION_TEXT = "Testing Description"; // This is the test description that is used to test the description textbox functionality
    public static final boolean TEST_DETAIL_PAGE_FIELD_NAMES_ALL = true; // true means the tests for field names on details page will run for all items (whether it is workunits or logical files) and false means it will only run for the first item
    public static final boolean TEST_WU_DETAIL_PAGE_DESCRIPTION_ALL = false; // true means the tests for checking the description textbox functionality on details page will run for all workunits and false means it will only run for the first workunit
    public static final boolean TEST_WU_DETAIL_PAGE_PROTECTED_ALL = false; // true means the tests for checking the protected checkbox functionality on details page will run for all workunits and false means it will only run for the first workunit
    public static final boolean TEST_DETAIL_PAGE_TAB_CLICK_ALL = false; // true means the tests for tab click validity on details page will run for all items (whether it is workunits or logical files) and false means it will only run for the first item

    // these values are set in the beginning in the TestRunner.java file
    public static String PATH_FOLDER_JSON = ""; // path of the folder of JSON files, it is passed in the CLI arguments.
    public static String LOG_LEVEL = ""; // log level is also passed in the CLI arguments, it could be "debug" or "detail"

    public static final String WORKUNITS_JSON_FILE_NAME = "workunits.json"; // name of the workunits JSON file stored in the above JSON folder path
    public static final String DFU_WORKUNITS_JSON_FILE_NAME = "dfu-workunits.json"; // name of the dfu-workunits JSON file stored in the above JSON folder path
    public static final String FILES_JSON_FILE_NAME = "files.json"; // name of the files JSON file stored in the above JSON folder path
}
