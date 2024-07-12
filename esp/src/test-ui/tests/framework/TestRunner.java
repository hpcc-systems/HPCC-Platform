package framework;

import org.openqa.selenium.Capabilities;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.remote.RemoteWebDriver;

public class TestRunner {

    public static void main(String[] args) {
        System.setProperty("webdriver.chrome.silentOutput", "true");
        System.setProperty("webdriver.chrome.driver", "/usr/bin/chromedriver");
        //System.setProperty("webdriver.chrome.driver", "C:/Users/nisha/Documents/Internship/Work/jars/chromeDriver/chromedriver.exe");
        // java.util.logging.Logger.getLogger("org.openqa.selenium").setLevel(Level.OFF);

        ChromeOptions chromeOptions = new ChromeOptions();
        chromeOptions.addArguments("--headless");
        chromeOptions.addArguments("--no-sandbox");
        chromeOptions.addArguments("--log-level=3");

        WebDriver driver = null;

        try {

            driver = new ChromeDriver(chromeOptions);

            Capabilities caps = ((RemoteWebDriver) driver).getCapabilities();

            String browserName = caps.getBrowserName();
            System.out.println("browserName: " + browserName);
            //String browserVersion = caps.getVersion();
            // System.out.println(browserName+" "+browserVersion);
        } catch (Exception ex) {
            System.out.println("Exception in driver initialization: " + ex.getMessage());
        }

        try {

            if (driver != null) {
                driver.get("http://127.0.0.1:8010/");
                //driver.get("https://play.hpccsystems.com:18010/esp/files/index.html#/activities");

                Thread.sleep(1000);

                if (driver.getPageSource().contains("Job Name")) {
                    System.out.println("Pass");
                } else {
                    System.err.println("Fail");
                }
                if (driver.getPageSource().contains("Owner")) {
                    System.out.println("Pass");
                } else {
                    System.err.println("Fail");
                }
                if (driver.getPageSource().contains("Target/Wuid")) {
                    System.out.println("Pass");
                } else {
                    System.err.println("Fail");
                }

                driver.quit();

            }else{
                System.out.println("Error: Driver did not initialize");
            }

        } catch (Exception ex) {
            System.out.println("Exception in loading web page: " + ex.getMessage());
        }
    }
}
