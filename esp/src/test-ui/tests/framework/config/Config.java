package framework.config;

public class Config {

    public static final String LOG_FILE_ERROR = "error_ecl_test.log";
    public static final String LOG_FILE_EXCEPTION = "exception_ecl_test.log";
    public static final String LOG_FILE_DEBUG = "debug_ecl_test.log";
    public static final String LOG_FILE_DETAIL = "detail_ecl_test.log";
    public static final String LOCAL_OS = "Windows";
    public static final String LOCAL_USER_PROFILE = "C:\\Users\\nisha";
    public static final String PATH_LOCAL_CHROME_DRIVER = "C:/Users/nisha/Documents/Internship/Work/jars/chromeDriver/chromedriver.exe";
    public static final String PATH_GH_ACTION_CHROME_DRIVER = "/usr/bin/chromedriver";
    public static final int[] dropdownValues = {10, 25, 50, 100, 250, 500, 1000};
    public static final int MALFORMED_TIME_STRING = -1;
    public static final int WAIT_TIME_IN_SECONDS = 1;
    public static final int WAIT_TIME_THRESHOLD_IN_SECONDS = 20;
    public static final String TEST_DESCRIPTION_TEXT = "Testing Description";
    public static final boolean TEST_DETAIL_PAGE_FIELD_NAMES_ALL = true;
    public static final boolean TEST_WU_DETAIL_PAGE_DESCRIPTION_ALL = true;
    public static final boolean TEST_WU_DETAIL_PAGE_PROTECTED_ALL = true;
    public static final boolean TEST_DETAIL_PAGE_TAB_CLICK_ALL = true;

    // these values are set in the beginning in the TestRunner.java file
    public static String PATH_FOLDER_JSON = "";
    public static String LOG_LEVEL = "";

    public static final String WORKUNITS_JSON_FILE_NAME = "workunits.json";
    public static final String DFU_WORKUNITS_JSON_FILE_NAME = "dfu-workunits.json";
    public static final String FILES_JSON_FILE_NAME = "files.json";
}
