package framework.pages;

import framework.config.URLConfig;
import framework.model.NavigationWebElement;
import framework.model.URLMapping;
import framework.utility.Common;
import org.openqa.selenium.By;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebElement;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static framework.config.URLConfig.urlMap;

public class ActivitiesTest {

    static final String[] textArray = {"Target/Wuid", "Graph", "State", "Owner", "Job Name"};

    @Test
    public void testActivitiesPage() {
        if(!Common.openWebPage(urlMap.get(URLConfig.NAV_ACTIVITIES).getUrl())){
            return;
        }

        Common.logDebug("Tests started for: Activities page.");

        testForAllText();

        List<NavigationWebElement> navWebElements = getNavWebElements();

        testForNavigationLinks(navWebElements);

        Common.logDebug("\nTests finished for: Activities page.");
        Common.logDebug("\nURL Map Generated: " + urlMap);
    }

    private void testForNavigationLinks(List<NavigationWebElement> navWebElements) {

        Common.logDebug("\nTests started for: Activities page: Testing Navigation Links");

        for (NavigationWebElement element : navWebElements) {

            try {
                element.webElement().click();

                if (testTabsForNavigationLinks(element)) {
                    String msg = "Success: Navigation Menu Link for " + element.name() + ". URL : " + element.hrefValue();
                    Common.logDetail(msg);
                } else {
                    String currentPage = getCurrentPage();
                    String errorMsg = "Failure: Navigation Menu Link for " + element.name() + " page failed. The current navigation page that we landed on is " + currentPage + ". Current URL : " + element.hrefValue();
                    Common.logError(errorMsg);
                }
            } catch (Exception ex) {
                Common.logException("Failure: Exception in Navigation Link for " + element.name() + ". URL : " + element.hrefValue() + " Error: " + ex.getMessage(), ex);
            }
        }
    }

    private String getCurrentPage() {

        for (var entry : URLConfig.tabsListMap.entrySet()) {

            List<String> tabs = entry.getValue();
            boolean allTabsPresent = true;
            for (String tab : tabs) {
                if (!Common.driver.getPageSource().contains(tab)) {
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

    private boolean testTabsForNavigationLinks(NavigationWebElement element) {
        List<String> tabsList = URLConfig.tabsListMap.get(element.name());

        for (String tab : tabsList) {
            try {
                WebElement webElement = Common.waitForElement(By.xpath("//a[text()='" + tab + "']"));
                urlMap.get(element.name()).getUrlMappings().put(tab, new URLMapping(tab, webElement.getAttribute("href")));
            } catch (TimeoutException ex) {
                return false;
            }
        }

        return true;
    }

    private List<NavigationWebElement> getNavWebElements() {

        List<NavigationWebElement> navWebElements = new ArrayList<>();

        for (String navName : URLConfig.navNamesArray) {
            try {
                WebElement webElement = Common.driver.findElement(By.name(navName)).findElement(By.tagName("a"));
                String hrefValue = webElement.getAttribute("href");
                navWebElements.add(new NavigationWebElement(navName, hrefValue, webElement));

                urlMap.put(navName, new URLMapping(navName, hrefValue));
            } catch (Exception ex) {
                Common.logException("Failure: Activities Page for Navigation Element: " + navName + ": Error: " + ex.getMessage(), ex);
            }
        }

        return navWebElements;
    }

    private void testForAllText() {
        Common.logDebug("\nTests started for: Activities page: Testing Text");
        for (String text : textArray) {
            Common.checkTextPresent(text, "Activities Page");
        }
    }
}
