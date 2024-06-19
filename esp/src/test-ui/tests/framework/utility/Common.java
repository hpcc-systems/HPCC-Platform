package framework.utility;

import framework.config.Config;
import org.openqa.selenium.WebDriver;

import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Common {

    public static void checkTextPresent(WebDriver driver, String text, String page, Logger errorLogger, Logger specificLogger) {
        if (driver.getPageSource().contains(text)) {
            logDetail(specificLogger, "Success: " + page + ": Text present: " + text);
        } else {
            logError(errorLogger, "Failure: " + page + ": Text not present: " + text);
        }
    }

    public static void openWebPage(WebDriver driver, String url) {

        driver.get(url);
        driver.manage().window().maximize();
        sleep();
    }

    public static void sleep() {
        try {
            Thread.sleep(Duration.ofSeconds(Config.WAIT_TIME_IN_SECONDS));
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        }
    }

    public static boolean isRunningOnLocal() {
        return System.getProperty("os.name").startsWith(Config.LOCAL_OS) && System.getenv("USERPROFILE").startsWith(Config.LOCAL_USER_PROFILE);
    }

    public static String getUrl(String url) {

        return isRunningOnLocal() ? Config.LOCAL_IP + url : Config.GITHUB_ACTION_IP + url;
    }

    public static void logError(Logger logger, String msg) {

        System.out.println(msg);
        logger.severe(msg);
    }

    public static void logDebug(Logger logger, String msg) {

        System.out.println(msg);

        if (logger != null && logger.getLevel() == Level.INFO) {
            logger.info(msg);
        }

        if (logger != null && logger.getLevel() == Level.FINE) {
            logger.fine(msg);
        }
    }

    public static void logDetail(Logger logger, String msg) {

        System.out.println(msg);

        if (logger != null && logger.getLevel() == Level.FINE) {
            logger.fine(msg);
        }
    }
}
