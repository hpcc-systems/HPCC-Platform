package framework.setup;

import org.openqa.selenium.WebDriver;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestResult;

import java.util.logging.Logger;

public class TestInjector implements IInvokedMethodListener {
    private final Logger logger;
    private final WebDriver driver;

    public TestInjector(Logger logger, WebDriver driver) {
        this.logger = logger;
        this.driver = driver;
    }

    @Override
    public void beforeInvocation(IInvokedMethod method, ITestResult testResult) {
        if (method.isTestMethod()) {
            LoggerHolder.setLogger(logger);
            WebDriverHolder.setDriver(driver);
        }
    }

    @Override
    public void afterInvocation(IInvokedMethod method, ITestResult testResult) {
        // Optionally, clean up if necessary
    }
}
