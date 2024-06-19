package framework.pages;

import framework.config.Config;
import framework.model.NavigationWebElement;
import framework.setup.LoggerHolder;
import framework.setup.WebDriverHolder;
import framework.utility.Common;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ActivitiesTest {

    static final String[] textArray = {"Target/Wuid", "Graph", "State", "Owner", "Job Name"};
    static final String[] navNamesArray = {"Activities", "ECL", "Files", "Published Queries", "Operations"};
    static final Map<String, List<String>> tabsListMap = Map.of(
            "Activities", List.of("Activities", "Event Scheduler"),
            "ECL", List.of("Workunits", "Playground"),
            "Files", List.of("Logical Files", "Landing Zones", "Workunits", "XRef (L)"),
            "Published Queries", List.of("Queries", "Package Maps"),
            "Operations", List.of("Topology (L)", "Disk Usage (L)", "Target Clusters (L)", "Cluster Processes (L)", "System Servers (L)", "Security (L)", "Dynamic ESDL (L)")
            //"DummyNavName", List.of("DummyTab1", "DummyTab2")
    );

    Logger errorLogger, specificLogger;

    @Test
    public void testActivitiesPage() {
        WebDriver driver = WebDriverHolder.getDriver();
        Common.openWebPage(driver, Common.getUrl(Config.ACTIVITIES_URL));

        errorLogger = LoggerHolder.getErrorLogger();
        specificLogger = LoggerHolder.getSpecificLogger();

        Common.logDebug(specificLogger, "Tests started for Activities Page");

        testForAllText(driver);

        List<NavigationWebElement> navWebElements = getNavWebElements(driver);

        testForNavigationLinks(driver, navWebElements);

        Common.logDebug(specificLogger, "Tests finished for Activities Page");
    }

    private void testForNavigationLinks(WebDriver driver, List<NavigationWebElement> navWebElements) {

        for (NavigationWebElement element : navWebElements) {
            element.webElement().click();

            if (testTabsForNavigationLinks(driver, element)) {
                Common.logDetail(specificLogger, "Success: Navigation Menu Link for " + element.name() + ". URL : " + element.hrefValue());
            } else {
                String currentPage = getCurrentPage(driver);
                Common.logError(errorLogger, "Failure: Navigation Menu Link for " + element.name() + " page failed. The current navigation page that we landed on is " + currentPage + ". Current URL : " + element.hrefValue());
            }
        }
    }

    private String getCurrentPage(WebDriver driver) {

        for (var entry : tabsListMap.entrySet()) {

            List<String> tabs = entry.getValue();
            boolean allTabsPresent = true;
            for (String tab : tabs) {
                if (!driver.getPageSource().contains(tab)) {
                    allTabsPresent = false;
                    break;
                }
            }

            if (allTabsPresent) {
                return entry.getKey();
            }
        }

        return "Invalid Page";
    }

    private boolean testTabsForNavigationLinks(WebDriver driver, NavigationWebElement element) {
        List<String> tabsList = tabsListMap.get(element.name());
        String pageSource = driver.getPageSource();

        for (String tab : tabsList) {
            if (!pageSource.contains(tab)) {
                return false;
            }
        }

        return true;
    }

    private List<NavigationWebElement> getNavWebElements(WebDriver driver) {

        List<NavigationWebElement> navWebElements = new ArrayList<>();

        for (String navName : navNamesArray) {
            WebElement webElement = driver.findElement(By.name(navName)).findElement(By.tagName("a"));
            String hrefValue = webElement.getAttribute("href");
            navWebElements.add(new NavigationWebElement(navName, hrefValue, webElement));
        }

        //navWebElements.add(new NavigationWebElement("DummyNavName", "http://192.168.0.221:8010/esp/files/index.html#/files", navWebElements.get(2).webElement()));

        return navWebElements;
    }

    private void testForAllText(WebDriver driver) {

        for (String text : textArray) {
            Common.checkTextPresent(driver, text, "Activities Page", errorLogger, specificLogger);
        }
    }
}
