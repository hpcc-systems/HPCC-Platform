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

        Common.logDebug("Tests finished for: Activities page.");
        Common.logDebug("URL Map Generated: " + urlMap);
    }

    private void testForNavigationLinks(List<NavigationWebElement> navWebElements) {

        Common.logDebug("Tests started for: Activities page: Testing Navigation Links");

        for (NavigationWebElement element : navWebElements) {

            StringBuilder errorMsg = new StringBuilder("OK");

            try {
                element.webElement().click();

                if (testTabsForNavigationLinks(element, errorMsg)) {
                    String msg = "Success: Navigation Menu Link for " + element.name() + ". URL : " + element.hrefValue();
                    Common.logDetail(msg);
                    if ( errorMsg.length() > 0 ){
                        String currentPage = getCurrentPage();
                        String warningMsg = "  Warning: on '" + currentPage + "' page there is missing/not matching element(s):" + errorMsg;
                        Common.logDetail(warningMsg);
                    }
                } else {
                    // Needs to report why it is failed
                    String currentPage = getCurrentPage();
                    String failureMsg = "Failure: Navigation Menu Link for " + element.name() + " page failed. The current navigation page that we landed on is " + currentPage + ". Current URL : " + element.hrefValue() + ". Missing element(s): " + errorMsg;
                    Common.logError(failureMsg);
                    // Needs to log the error into the logDetails as well. 
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

    private boolean testTabsForNavigationLinks(NavigationWebElement element, StringBuilder msg) {
        msg.delete(0, msg.length());
        boolean retVal = true;
                
        List<String> tabsList = URLConfig.tabsListMap.get(element.name());
        int numOfElements = tabsList.size();
        if ( numOfElements == 0 ) {
            return retVal;
        }

        double elementFound = 0.0;  // It is double for calculating ratio later.

        for (String tab : tabsList) {
            try {
                WebElement webElement = Common.waitForElement(By.xpath("//a[text()='" + tab + "']"));
                urlMap.get(element.name()).getUrlMappings().put(tab, new URLMapping(tab, webElement.getAttribute("href")));
                elementFound += 1.0;
            } catch (TimeoutException ex) {
                msg.append("'" + tab + "', ");
            }
        }
        
        if ( (elementFound / numOfElements) < 0.5 ) {
            retVal = false;
        }
        Common.logDebug("In testTabsForNavigationLinks(element = '" + element.name() + "') -> numOfElements:" + numOfElements + ", elementFound:" + elementFound + ", ratio: " + (elementFound / numOfElements) + ", retVal:" + retVal + "." );
        return retVal;
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
        Common.logDebug("Tests started for: Activities page: Testing Text");
        for (String text : textArray) {
            Common.checkTextPresent(text, "Activities Page");
        }
    }
}
