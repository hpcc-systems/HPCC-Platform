package framework.setup;

import org.openqa.selenium.WebDriver;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestResult;

import java.util.logging.Logger;

public class TestInjector implements IInvokedMethodListener {
    private final Logger errorLogger;
    private final Logger specificLogger;
    private final WebDriver driver;

    public TestInjector(Logger errorLogger, Logger specificLogger, WebDriver driver) {
        this.errorLogger = errorLogger;
        this.specificLogger = specificLogger;
        this.driver = driver;
    }

    @Override
    public void beforeInvocation(IInvokedMethod method, ITestResult testResult) {
        if (method.isTestMethod()) {
            LoggerHolder.setErrorLogger(errorLogger);
            LoggerHolder.setSpecificLogger(specificLogger);
            WebDriverHolder.setDriver(driver);
        }
    }

    @Override
    public void afterInvocation(IInvokedMethod method, ITestResult testResult) {
        // Optionally, clean up if necessary
    }
}
