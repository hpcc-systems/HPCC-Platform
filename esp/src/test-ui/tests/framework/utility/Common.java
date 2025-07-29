package framework.utility;

import framework.config.Config;
import framework.config.URLConfig;
import org.openqa.selenium.By;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.File;

public class Common {

    public static WebDriver driver;
    public static Logger errorLogger = setupLogger("error");
    public static Logger exceptionLogger = setupLogger("exception");
    public static Logger specificLogger;
    public static int num_exceptions = 0;
    public static int num_errors = 0;
    public static Boolean isRunningOnLocal = false;

    public static void checkTextPresent(String text, String page) {
        try {
            WebElement element = Common.waitForElement(By.xpath("//*[text()='" + text + "']"));
            if (element != null) {
                String msg = "Success: " + page + ": Text present: " + text;
                logDetail(msg);
            }
        } catch (TimeoutException ex) {
            String errorMsg = "Failure: " + page + ": Text not present: " + text;
            logError(errorMsg);
        }
    }

    public static boolean openWebPage(String url) {
        try {
            driver.get(url);
            driver.manage().window().maximize();
            sleep();
            return true;
        } catch (Exception ex) {
            Common.logException("Error in opening web page: " + url, ex);
            return false;
        }
    }

    public static void sleep() {
        try {
            Thread.sleep(Duration.ofSeconds(Config.WAIT_TIME_IN_SECONDS));
        } catch (InterruptedException e) {
            Common.logException("Error in sleep: " + e.getMessage(), e);
        }
    }

    public static void sleepWithTime(int seconds) {
        try {
            Thread.sleep(Duration.ofSeconds(seconds));
        } catch (InterruptedException e) {
            Common.logException("Error in sleep: " + e.getMessage(), e);
        }
    }

    public static void checkIfLocalConfigIsSet() {
        String gitHubWorkspace = System.getenv("GITHUB_WORKSPACE");

        if (Config.LOG_LEVEL == "debug" ){
            String osName = System.getProperty("os.name");
            System.out.println("OS name: '" + osName + "'");

            String userProfile = System.getenv("USERPROFILE");
            System.out.println("USERPROFILE: '" + System.getenv("USERPROFILE") + "'");

            String userName = System.getenv("USERNAME");
            System.out.println("USERNAME: '" + System.getenv("USERNAME") + "'");

            System.out.println("GITHUB_WORKSPACE: '" + System.getenv("GITHUB_WORKSPACE") + "'");
        }

        Boolean localSettingsIsOK = true;

        if ( gitHubWorkspace == null )
        {
            // Definitely not running in GitHub Action environment need all LOCAL_* variable set
            isRunningOnLocal = true;

            if ( Config.PATH_LOCAL_CHROME_DRIVER.startsWith("<") ) {
                System.out.println("The current value of 'PATH_LOCAL_CHROME_DRIVER' in 'Config.java'\n is '" + Config.PATH_LOCAL_CHROME_DRIVER + "'.\n");
                System.out.println("You need to set it to the correct path.\n");
                localSettingsIsOK = false;
            } else {
                // Need to check whether the CHROME DRIVER file is exist.
                File f = new File(Config.PATH_LOCAL_CHROME_DRIVER);
                if ( ! f.exists() || ! f.isFile() ){
                    System.out.println("The PATH_LOCAL_CHROME_DRIVER = '"+ Config.PATH_LOCAL_CHROME_DRIVER + "' in 'Config.java'\n is not exist or not a file.\n");
                    localSettingsIsOK = false;
                }
            }

            if ( URLConfig.LOCAL_ECLWATCH_IP.startsWith("<") ) {
                System.out.println("The current value of 'LOCAL_ECLWATCH_IP' in 'URLConfig.java'\n is '" + URLConfig.LOCAL_ECLWATCH_IP + "'.\n");
                System.out.println("You need to set it to correct 'IP:PORT' value.\n");
                localSettingsIsOK = false;
            }

            if (  localSettingsIsOK == false) {
                System.out.println("Please, fix the problem(s) above and try again.\n");
                System.exit(1);
            }
        }
        System.out.print("Test running on ");
        if (isRunningOnLocal)
        {
            System.out.println("local environment.\n");
        }
        else {
            System.out.println("GitHub environment.\n");        
        }
    }


    public static String getIP() {

        return isRunningOnLocal ? URLConfig.LOCAL_ECLWATCH_IP : URLConfig.GITHUB_ACTION_ECLWATCH_IP;
    }

    public static WebElement waitForElement(By locator) {
        return new WebDriverWait(driver, Duration.ofSeconds(Config.WAIT_TIME_THRESHOLD_IN_SECONDS)).until(ExpectedConditions.presenceOfElementLocated(locator));
    }

    public static void waitForElementToBeClickable(WebElement element) {
        new WebDriverWait(driver, Duration.ofSeconds(Config.WAIT_TIME_THRESHOLD_IN_SECONDS)).until(ExpectedConditions.elementToBeClickable(element));
    }

    // aria-disabled is a standard attribute and is widely used in HTML to indicate whether an element is disabled. It is system-defined, meaning it is part of the standard HTML specifications and not a custom class or attribute that might change frequently.
    // By using aria-disabled, you ensure that the check for the disabled state is consistent and less likely to break due to UI changes. Custom class names or attributes defined by developers can change frequently during updates or redesigns, but standard attributes like aria-disabled are much more stable.

    public static void waitForElementToBeDisabled(WebElement element) {
        new WebDriverWait(driver, Duration.ofSeconds(Config.WAIT_TIME_THRESHOLD_IN_SECONDS))
                .until(ExpectedConditions.attributeContains(element, "aria-disabled", "true"));
    }

    public static void logError(String message) {
        System.err.println(message);
        errorLogger.severe(message);
        num_errors++;
    }

    public static void logException(String message, Exception ex) {

        message += Arrays.toString(ex.getStackTrace());

        System.err.println(message);
        exceptionLogger.severe(message);
        num_exceptions++;
    }

    public static void logDebug(String message) {
        // Usin FINE level for Debug logs
        if (specificLogger != null && specificLogger.getLevel() == Level.FINE) {
                specificLogger.fine(message);
                System.out.println(message);
        }
    }

    public static void logDetail(String message) {
        if (specificLogger != null && ( (specificLogger.getLevel() == Level.INFO ) || (specificLogger.getLevel() == Level.FINE)) ) {
            specificLogger.fine(message);
            System.out.println(message);
        }
    }

    public static void initializeLoggerAndDriver() {
        specificLogger = setupLogger(Config.LOG_LEVEL);
        driver = setupWebDriver();
    }

    public static void printNumOfErrorsAndExceptions() {
        System.out.println("Total number of exceptions recorded: " + num_exceptions);
        System.out.println("Total number of errors recorded: " + num_errors);
    }

    private static WebDriver setupWebDriver() {

        try {
            ChromeOptions chromeOptions = new ChromeOptions();

            if (Config.SHOW_CHROME) {
               if (Common.isRunningOnLocal){
                   System.out.println("Chrome window will be displayed during the tests.");
               }
               else
               {
                   System.out.println("Test doesn't run in local/interactive environment so, the '-v' (visual) CLI parameter is ignored.");
                   chromeOptions.addArguments("--headless"); // sets the ChromeDriver to run in headless mode, meaning it runs without opening a visible browser window.
                }
	        }
	        else {
	            chromeOptions.addArguments("--headless"); // sets the ChromeDriver to run in headless mode, meaning it runs without opening a visible browser window.
    	    }
            chromeOptions.addArguments("--no-sandbox"); // disables the sandbox security feature in Chrome.
            chromeOptions.addArguments("--log-level=3"); // sets the log level for the ChromeDriver. Level 3 corresponds to errors only.

            System.setProperty("webdriver.chrome.silentOutput", "true"); // suppresses the logs generated by the ChromeDriver

            if (Common.isRunningOnLocal) {
                System.setProperty("webdriver.chrome.driver", Config.PATH_LOCAL_CHROME_DRIVER); // sets the system property to the path of the ChromeDriver executable.
            } else {
                System.setProperty("webdriver.chrome.driver", Config.PATH_GH_ACTION_CHROME_DRIVER);
            }

            return new ChromeDriver(chromeOptions);
        } catch (Exception ex) {
            logException("Failure: Error in setting up web driver: " + ex.getMessage(), ex);
        }

        return null;
    }

    private static Logger setupLogger(String logLevel) {

        if (logLevel.isEmpty()) {
            return null;
        }

        Logger logger = Logger.getLogger(logLevel);
        logger.setUseParentHandlers(false); // Disable console logging

        try {
            FileHandler fileHandler;
            CustomFormatter formatter = new CustomFormatter();

            if (logLevel.equalsIgnoreCase("error")) {
                fileHandler = new FileHandler(Config.LOG_FILE_ERROR);
                fileHandler.setFormatter(formatter);
                logger.addHandler(fileHandler);
                logger.setLevel(Level.SEVERE);
            } else if (logLevel.equalsIgnoreCase("exception")) {
                fileHandler = new FileHandler(Config.LOG_FILE_EXCEPTION);
                fileHandler.setFormatter(formatter);
                logger.addHandler(fileHandler);
                logger.setLevel(Level.SEVERE);
            } else if (logLevel.equalsIgnoreCase("debug")) {
                fileHandler = new FileHandler(Config.LOG_FILE_DEBUG);
                fileHandler.setFormatter(formatter);
                logger.addHandler(fileHandler);
                logger.setLevel(Level.FINE);
            } else if (logLevel.equalsIgnoreCase("detail")) {
                fileHandler = new FileHandler(Config.LOG_FILE_DETAIL);
                fileHandler.setFormatter(formatter);
                logger.addHandler(fileHandler);
                logger.setLevel(Level.INFO);
            }
        } catch (IOException e) {
            System.err.println("Failure: Failed to setup logger: " + e.getMessage());
        }

        Logger.getLogger("org.openqa.selenium").setLevel(Level.OFF); // Turn off all logging from the Selenium WebDriver.
        return logger;
    }
}
