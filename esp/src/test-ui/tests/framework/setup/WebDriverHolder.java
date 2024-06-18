package framework.setup;

import org.openqa.selenium.WebDriver;

public class WebDriverHolder {
    private static WebDriver driver;

    public static WebDriver getDriver() {
        return driver;
    }

    public static void setDriver(WebDriver driver) {
        WebDriverHolder.driver = driver;
    }
}
