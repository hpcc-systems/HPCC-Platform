package framework.utility;

import framework.config.Config;
import org.openqa.selenium.WebDriver;

import java.time.Duration;
import java.util.logging.Logger;

public class Common {

    public static void checkTextPresent(WebDriver driver, String text, String page, Logger logger) {
        if (driver.getPageSource().contains(text)) {
            String msg = "Success: " + page + ": Text present: " + text;
            System.out.println(msg);
        } else {
            String errorMsg = "Failure: " + page + ": Text not present: " + text;
            System.err.println(errorMsg);
            logger.severe(errorMsg);
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
}
