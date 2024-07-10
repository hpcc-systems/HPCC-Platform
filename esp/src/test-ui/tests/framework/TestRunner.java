package framework;

import org.openqa.selenium.Capabilities;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.remote.RemoteWebDriver;

import java.io.IOException;
import java.net.URI;

public class TestRunner {

    public static void main(String[] args) throws IOException, InterruptedException {
        System.setProperty("webdriver.chrome.silentOutput", "true");
        System.setProperty("webdriver.chrome.driver", "/usr/bin/chromedriver");
        // java.util.logging.Logger.getLogger("org.openqa.selenium").setLevel(Level.OFF);

        ChromeOptions chromeOptions = new ChromeOptions();
        chromeOptions.addArguments("--headless");
        chromeOptions.addArguments("--no-sandbox");
        chromeOptions.addArguments("--log-level=3");

        WebDriver driver = new RemoteWebDriver(URI.create("http://localhost:4444/wd/hub").toURL(), chromeOptions);

        Capabilities caps = ((RemoteWebDriver) driver).getCapabilities();

        String browserName = caps.getBrowserName();
        //String browserVersion = caps.getVersion();
        // System.out.println(browserName+" "+browserVersion);

        driver.get("http://127.0.0.1:8010/");

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
    }
}
