import java.io.IOException;
 
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.Capabilities;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.testng.annotations.Test;
 
public class Activities2 {
 
    public static void main(String[] args) throws IOException, InterruptedException {
        System.setProperty("webdriver.chrome.silentOutput", "true");
        System.setProperty("webdriver.chrome.driver", "/usr/bin/chromedriver");
        // java.util.logging.Logger.getLogger("org.openqa.selenium").setLevel(Level.OFF);

        ChromeOptions chromeOptions = new ChromeOptions();
        chromeOptions.addArguments("--headless");
        chromeOptions.addArguments("--no-sandbox");
        chromeOptions.addArguments("--log-level=3");

        WebDriver driver = new ChromeDriver(chromeOptions);
        
        Capabilities caps = ((RemoteWebDriver) driver).getCapabilities();

        String browserName = caps.getBrowserName();
        String browserVersion = caps.getVersion();
        // System.out.println(browserName+" "+browserVersion);

        driver.get(args[0]);

        Thread.sleep(1000);

        int errorCount = 0;

        if (driver.getPageSource().contains("Job Name")) {
                System.out.println("Pass");
        } else {
                System.err.println("Fail");
                ++errorCount;
        }
        if (driver.getPageSource().contains("Owner")) {
                System.out.println("Pass");
        } else {
                System.err.println("Fail");
                ++errorCount;
        }
        if (driver.getPageSource().contains("Target\\Wuid")) {
                System.out.println("Pass");
        } else {
                System.err.println("Fail");
                ++errorCount;
        }

        driver.quit();

        System.exit(errorCount);
    }
}
