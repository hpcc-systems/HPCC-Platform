package framework.pages;

import framework.config.Config;
import framework.utility.Common;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;


// This class is the super class for all the those web pages that contains a tabular data for e.g. workunits, files, queries etc. It also contains the tests for their respective details page including testing text, content and tabs.

public abstract class BaseTableTest<T> {

    protected abstract String getPageName();

    protected abstract String getPageUrl();

    protected abstract String getJsonFilePath();

    protected abstract String getSaveButtonDetailsPage();

    protected abstract String[] getColumnNames();

    protected abstract String[] getDetailNames();

    protected abstract String[] getColumnKeys();

    protected abstract String[] getDetailKeys();

    protected abstract String getCheckboxTypeForDetailsPage();

    protected abstract String getAttributeTypeForDetailsPage();

    protected abstract String getAttributeValueForDetailsPage();

    protected abstract String[] getDetailKeysForPageLoad();

    protected abstract String getUniqueKeyName();

    protected abstract String getUniqueKey();

    protected abstract String[] getColumnKeysWithLinks();

    protected abstract Object parseDataUIValue(Object dataUIValue, Object dataJSONValue, String columnName, Object dataIDUIValue);

    protected abstract Object parseDataJSONValue(Object dataJSONValue, String columnName, Object dataIDUIValue);

    protected abstract List<T> parseJson(String filePath) throws Exception;

    protected abstract Object getColumnDataFromJson(T object, String columnKey);

    protected abstract void sortJsonUsingSortOrder(String currentSortOrder, String columnKey);

    protected abstract String getCurrentPage();

    protected abstract Map<String, T> getJsonMap();

    protected abstract Map<String, List<String>> getColumnNamesForTabsDetailsPage();

    protected abstract String[] getTabValuesForDetailsPage();

    protected abstract void testDetailSpecificFunctionality(String name, int i);

    protected List<T> jsonObjects;

    protected void testPage() {
        if(!Common.openWebPage(getPageUrl())){
            return;
        }

        try {
            Common.logDebug("Tests started for: " + getPageName() + " page.");

            testForAllText();

            jsonObjects = getAllObjectsFromJson();
            if (jsonObjects != null) {

                int numOfItemsJSON = jsonObjects.size();
                clickDropdown(numOfItemsJSON);

                testContentAndSortingOrder();
            }

            testLinksInTable();

            Common.logDebug("Tests finished for: " + getPageName() + " page.");

        } catch (Exception ex) {
            Common.logException("Error: " + getPageName() + ": Exception: " + ex.getMessage(), ex);
        }
    }

    private void testDetailsPage(String name, int i) {

        if (Config.TEST_DETAIL_PAGE_FIELD_NAMES_ALL) { // TEST_DETAIL_PAGE_FIELD_NAMES_ALL = true means the test will run for all items and false means it will only run for the first item
            testForAllTextInDetailsPage(name);
        } else if (i == 0) {
            testForAllTextInDetailsPage(name);
        }

        testDetailsContentPage(name);
        testDetailSpecificFunctionality(name, i);

        if (Config.TEST_DETAIL_PAGE_TAB_CLICK_ALL) { // TEST_DETAIL_PAGE_TAB_CLICK_ALL = true means the test will run for all items and false means it will only run for the first item
            testTabClickOnDetailsPage(name);
        } else if (i == 0) {
            testTabClickOnDetailsPage(name);
        }
    }

    private void testTabClickOnDetailsPage(String name) {

        Common.logDebug("Test started for: Tab Click on " + getPageName() + " Details Page. For: " + name);

        waitToLoadDetailsPage();

        for (var tabValue : getTabValuesForDetailsPage()) {

            try {
                WebElement element = Common.driver.findElement(By.xpath("//button[@value='" + tabValue + "']"));
                javaScriptElementClick(element);
                testPresenceOfColumnNames(getColumnNamesForTabsDetailsPage().get(tabValue), tabValue);
            } catch (Exception ex) {
                Common.logException("Error: " + getPageName() + " Details Page. Testing tab click on: " + tabValue + " Error: " + ex.getMessage(), ex);
            }
        }

    }

    protected void javaScriptElementClick(WebElement element) {

        JavascriptExecutor js = (JavascriptExecutor) Common.driver;
        js.executeScript("arguments[0].click();", element);
    }

    private void testPresenceOfColumnNames(List<String> columnNames, String tabValue) {

        int numOfElements = columnNames.size();
        Common.logDebug("testPresenceOfColumnNames(" + numOfElements + " column names, tabValue:" + tabValue + ")");

        if ( numOfElements == 0 ) {
            return;
        }

        double elementFound = 0.0;  // It is double for calculating ratio later.
        String currentURL = Common.driver.getCurrentUrl();

        for (String columnName : columnNames) {
            Common.logDebug("columnName: " + columnName);

            try {
                Common.waitForElement(By.xpath("//*[text()='" + columnName + "']"));
                elementFound += 1.0;
            } catch (TimeoutException ex) {
                Common.logDebug("Warning: " + getPageName() + " Details Page. Text : " + columnName + " not present for tab: " + tabValue + ". Current URL: " + currentURL);
            }
        }

        double ratio = elementFound / numOfElements;
        if ( ratio < 0.5 ) {
            Common.logError("Failure: "+ getPageName() + " Details Page. Tab click on: '" + tabValue + "' -> numOfElements:" + numOfElements + ", elementFound:" + elementFound + ", ratio: " + String.format("%.2f",ratio) + "." );
        } else {
            Common.logDetail("Success: " + getPageName() + " Details Page. Tab click on: " + tabValue + " (ratio:" + String.format("%.2f",ratio) + ").");
        }
    }

    protected void clickOnSaveButton() {

        WebElement saveButton = getSaveButtonWebElementDetailsPage();
        Common.waitForElementToBeClickable(saveButton);
        saveButton.click();

        saveButton = getSaveButtonWebElementDetailsPage();
        Common.waitForElementToBeDisabled(saveButton);
    }

    private void testLinksInTable() {

        Common.logDebug("Tests started for: " + getPageName() + " page: Testing Links");

        Common.driver.navigate().refresh(); // refreshing page as page has scrolled to right for testing the sorting functionality of column headers, so bringing it back to normal view by refreshing it

        for (String columnKey : getColumnKeysWithLinks()) {

            List<Object> values = getDataFromUIUsingColumnKey(columnKey);

            for (int i = 0; i < values.size(); i++) {

                try {
                    String name = values.get(i).toString().trim();

                    WebElement element = Common.waitForElement(By.xpath("//div[text()='" + name + "']/.."));
                    String href = Common.driver.getCurrentUrl();

                    String dropdownValueBefore = getSelectedDropdownValue();
                    element.click();

                    if (Common.driver.getPageSource().contains(name)) {
                        String msg = "Success: " + getPageName() + ": Link Test Pass for " + (i + 1) + ". " + name + ". URL : " + href;
                        Common.logDetail(msg);
                        // after the link test has passed, the code tests the details page(including, the text, content, checkbox, description and tabs)
                        testDetailsPage(name, i);

                    } else {
                        String currentPage = getCurrentPage();
                        String errorMsg = "Failure: " + getPageName() + ": Link Test Fail for " + (i + 1) + ". " + name + " page failed. The current navigation page that we landed on is " + currentPage + ". Current URL : " + href;
                        Common.logError(errorMsg);
                    }

                    Common.driver.navigate().to(getPageUrl());
                    Common.driver.navigate().refresh();

                    String dropdownValueAfter = getSelectedDropdownValue();

                    // Log error if the dropdown value has changed
                    if (!dropdownValueBefore.equals(dropdownValueAfter)) {
                        String dropdownErrorMsg = "Failure: " + getPageName() + ": Dropdown value changed after navigating back. Before: " + dropdownValueBefore + ", After: " + dropdownValueAfter;
                        Common.logError(dropdownErrorMsg);
                    }
                } catch (Exception ex) {
                    Common.logException("Failure: " + getPageName() + ": Exception in testing links for column: " + columnKey + " value: " + values.get(i) + " Error: " + ex.getMessage(), ex);
                }
            }
        }
    }

    protected void waitToLoadDetailsPage() {

        int waitTimeInSecs = Config.WAIT_TIME_IN_SECONDS;

        boolean allVisible;

        do {
            Common.sleepWithTime(waitTimeInSecs);
            allVisible = true;

            for (String detailKey : getDetailKeysForPageLoad()) {
                WebElement element = Common.waitForElement(By.id(detailKey));
                if ("".equals(element.getAttribute(getAttributeValueForDetailsPage()))) {
                    allVisible = false;
                    break;
                }
            }

            if (allVisible) {
                break;
            }

            waitTimeInSecs++;

        } while (waitTimeInSecs < Config.WAIT_TIME_THRESHOLD_IN_SECONDS);

        Common.logDebug("Sleep Time In Seconds To Load Details Page: " + waitTimeInSecs);
    }

    private void testDetailsContentPage(String name) {
        Common.logDebug("Tests started for : " + getPageName() + " Details page: Testing Content: " + getUniqueKeyName() + " : " + name);
        try {
            waitToLoadDetailsPage(); // sleep until the specific detail fields have loaded
        } catch (Exception ex) {
            Common.logException("Error: " + getPageName() + ": Exception in waitToLoadDetailsPage: " + getUniqueKeyName() + " : " + name + " Exception: " + ex.getMessage(), ex);
        }

        boolean pass = true;

        T object = getJsonMap().get(name);

        for (String detailKey : getDetailKeys()) {
            try {
                WebElement element = Common.waitForElement(By.id(detailKey));

                Object dataUIValue, dataJSONValue;

                if (element.getAttribute(getAttributeTypeForDetailsPage()).equals(getCheckboxTypeForDetailsPage())) {
                    dataUIValue = element.isSelected();
                } else {
                    dataUIValue = element.getAttribute(getAttributeValueForDetailsPage());
                }

                dataJSONValue = getColumnDataFromJson(object, detailKey);

                dataUIValue = parseDataUIValue(dataUIValue, dataJSONValue, detailKey, name);
                dataJSONValue = parseDataJSONValue(dataJSONValue, detailKey, name);

                if (!dataUIValue.equals(dataJSONValue)) {
                    Common.logError("Failure: " + getPageName() + " Details page, Incorrect " + detailKey + " : " + dataUIValue + " in UI for " + getUniqueKeyName() + " : " + name + ". Correct " + detailKey + " is: " + dataJSONValue);
                    pass = false;
                }

            } catch (Exception ex) {
                Common.logException("Error: Details " + getPageName() + "Page, for: " + getUniqueKeyName() + " : " + name + " Error: " + ex.getMessage(), ex);
            }
        }

        if (pass) {
            Common.logDetail("Success: " + getPageName() + " Details page: All content test passed for: " + getUniqueKeyName() + " : " + name);
        }
    }

    private void testForAllTextInDetailsPage(String name) {
        Common.logDebug("Tests started for: " + getPageName() + " Details page: " + getUniqueKeyName() + ": " + name + ": Testing Text");
        for (String text : getDetailNames()) {
            Common.checkTextPresent(text, getPageName() + " Details page: " + getUniqueKeyName() + ": " + name);
        }
    }

    private void testContentAndSortingOrder() {

        Common.logDebug("Tests started for: " + getPageName() + " page: Testing Content");

        if (testTableContent()) {

            Common.logDebug("Tests started for: " + getPageName() + " page: Testing Sorting Order");

            for (int i = 0; i < getColumnKeys().length; i++) {
                try {
                    testTheSortingOrderForOneColumn(getColumnKeys()[i], getColumnNames()[i]);
                } catch (Exception ex) {
                    Common.logException("Error: " + getPageName() + ": Exception while testing sort order for column: " + getColumnNames()[i] + ": Exception:" + ex.getMessage(), ex);
                }
            }
        }
    }

    private void testTheSortingOrderForOneColumn(String columnKey, String columnName) {
        for (int i = 0; i < 3; i++) {

            String currentSortOrder = getCurrentSortingOrder(columnKey);
            if (currentSortOrder != null) {
                List<Object> columnDataFromUI = getDataFromUIUsingColumnKey(columnKey);

                sortJsonUsingSortOrder(currentSortOrder, columnKey);

                List<Object> columnDataFromJSON = getDataFromJSONUsingColumnKey(columnKey);
                List<Object> columnDataIDFromUI = getDataFromUIUsingColumnKey(getUniqueKey());

                if (compareData(columnDataFromUI, columnDataFromJSON, columnDataIDFromUI, columnName)) {
                    Common.logDebug("Success: " + getPageName() + ": Values are correctly sorted in " + currentSortOrder + " order by: " + columnName);
                } else {
                    String errMsg = "Failure: " + getPageName() + ": Values are not correctly sorted in " + currentSortOrder + " order by: " + columnName;
                    Common.logError(errMsg);
                }
            } else {
                Common.logError("Failure: " + getPageName() + " Unable to get sort order for column: " + columnKey);
            }
        }
    }

    private String getCurrentSortingOrder(String columnKey) {

        try {

            WebElement columnHeader = Common.driver.findElement(By.xpath("//*[@role='columnheader' and @*[.='" + columnKey + "']]"));

            // Scroll to bring the column header into view
            ((JavascriptExecutor) Common.driver).executeScript("arguments[0].scrollIntoView(true);", columnHeader);

            String oldSortOrder = columnHeader.getAttribute("aria-sort");

            columnHeader.click();

            return waitToLoadChangedSortOrder(oldSortOrder, columnKey);

        } catch (Exception ex) {
            Common.logException("Failure: " + getPageName() + " Exception in getting sort order for column: " + columnKey + " Error: " + ex.getMessage(), ex);
        }

        return null;
    }

    private String waitToLoadChangedSortOrder(String oldSortOrder, String columnKey) {

        int waitTimeInSecs = Config.WAIT_TIME_IN_SECONDS;
        String newSortOrder;

        do {
            Common.sleepWithTime(waitTimeInSecs);

            WebElement columnHeaderNew = Common.driver.findElement(By.xpath("//*[@role='columnheader' and @*[.='" + columnKey + "']]"));

            newSortOrder = columnHeaderNew.getAttribute("aria-sort");

            if (!Objects.equals(newSortOrder, oldSortOrder)) {
                break;
            }

            waitTimeInSecs++;

        } while (waitTimeInSecs < Config.WAIT_TIME_THRESHOLD_IN_SECONDS);

        return newSortOrder;
    }

    private List<Object> getDataFromJSONUsingColumnKey(String columnKey) {
        List<Object> columnDataFromJSON = new ArrayList<>();
        for (T jsonObject : jsonObjects) {
            columnDataFromJSON.add(getColumnDataFromJson(jsonObject, columnKey));
        }
        return columnDataFromJSON;
    }

    protected List<Object> getDataFromUIUsingColumnKey(String columnKey) {

        List<Object> columnData = new ArrayList<>();

        try {

            List<WebElement> elements = waitToLoadListOfAllUIObjects(columnKey);
            for (WebElement element : elements) {
                columnData.add(element.getText().trim());
            }

        } catch (Exception ex) {
            Common.logException("Failure: " + getPageName() + ": Error in getting data from UI using column key: " + columnKey + "Error: " + ex.getMessage(), ex);
        }

        return columnData;
    }

    private List<WebElement> waitToLoadListOfAllUIObjects(String columnKey) {

        int waitTimeInSecs = Config.WAIT_TIME_IN_SECONDS;
        List<WebElement> elements;

        do {

            elements = Common.driver.findElements(By.xpath("//*[@role='gridcell' and @*[.='" + columnKey + "']]"));

            if (elements.size() >= jsonObjects.size()) {
                break;
            }

            Common.sleepWithTime(waitTimeInSecs++);

        } while (waitTimeInSecs < Config.WAIT_TIME_THRESHOLD_IN_SECONDS);

        return elements;
    }

    @SuppressWarnings("unchecked")
    void ascendingSortJson(String columnKey) {
        try {
            jsonObjects.sort(Comparator.comparing(jsonObject -> (Comparable<Object>) getColumnDataFromJson(jsonObject, columnKey)));
        } catch (Exception ex) {
            Common.logException("Failure: " + getPageName() + ": Exception in sorting JSON in ascending order using column key: " + columnKey + " Error: " + ex.getMessage(), ex);
        }
    }

    @SuppressWarnings("unchecked")
    void descendingSortJson(String columnKey) {
        try {
            jsonObjects.sort(Comparator.comparing((Function<T, Comparable<Object>>) jsonObject -> (Comparable<Object>) getColumnDataFromJson(jsonObject, columnKey)).reversed());
        } catch (Exception ex) {
            Common.logException("Failure: " + getPageName() + ": Exception in sorting JSON in descending order using column key: " + columnKey + " Error: " + ex.getMessage(), ex);
        }
    }

    private boolean testTableContent() {
        Common.logDebug("Page: " + getPageName() + ": Number of Objects from Json: " + jsonObjects.size());

        List<Object> columnDataIDFromUI = getDataFromUIUsingColumnKey(getUniqueKey());

        Common.logDebug("Page: " + getPageName() + ": Number of Objects from UI: " + columnDataIDFromUI.size());

        if (jsonObjects.size() != columnDataIDFromUI.size()) {
            String errMsg = "Failure: " + getPageName() + ": Number of items on UI are not equal to the number of items in JSON" +
                    "\nNumber of Objects from Json: " + jsonObjects.size() +
                    "\nNumber of Objects from UI: " + columnDataIDFromUI.size();
            Common.logError(errMsg);
            return false;
        }

        boolean pass = true;

        for (int i = 0; i < getColumnKeys().length; i++) {
            List<Object> columnDataFromUI = getDataFromUIUsingColumnKey(getColumnKeys()[i]);
            List<Object> columnDataFromJSON = getDataFromJSONUsingColumnKey(getColumnKeys()[i]);
            if (!compareData(columnDataFromUI, columnDataFromJSON, columnDataIDFromUI, getColumnNames()[i])) {
                pass = false;
            }
        }

        return pass;
    }

    private List<T> getAllObjectsFromJson() {
        String filePath = getJsonFilePath();
        try {
            return parseJson(filePath);
        } catch (Exception ex) {
            Common.logException("Failure: " + getPageName() + " Unable to parse JSON File: Exception: " + ex.getMessage(), ex);
        }

        return null;
    }

    private boolean compareData(List<Object> dataUI, List<Object> dataJSON, List<Object> dataIDUI, String columnName) {

        boolean pass = true;

        for (int i = 0; i < dataUI.size(); i++) {

            Object dataUIValue = dataUI.get(i);
            Object dataJSONValue = dataJSON.get(i);
            Object dataIDUIValue = dataIDUI.get(i);

            dataUIValue = parseDataUIValue(dataUIValue, dataJSONValue, columnName, dataIDUIValue);
            dataJSONValue = parseDataJSONValue(dataJSONValue, columnName, dataIDUIValue);

            if (!checkValues(dataUIValue, dataJSONValue, dataIDUIValue, columnName)) {
                pass = false;
            }
        }

        if (pass) {
            Common.logDetail("Success: " + getPageName() + ": Content test passed for column: " + columnName);
        }

        return pass;
    }

    private boolean checkValues(Object dataUIValue, Object dataJSONValue, Object dataIDUIValue, String columnName) {
        if (!dataUIValue.equals(dataJSONValue)) {
            String errMsg = "Failure: " + getPageName() + ": Incorrect " + columnName + " : " + dataUIValue + " in UI for " + getUniqueKeyName() + " : " + dataIDUIValue + ". Correct " + columnName + " is: " + dataJSONValue;
            Common.logError(errMsg);
            return false;
        }

        return true;
    }

    private void clickDropdown(int numOfItemsJSON) {

        try {

            WebElement dropdown = Common.waitForElement(By.id("pageSize"));
            dropdown.click();

            WebDriverWait wait = new WebDriverWait(Common.driver, Duration.ofSeconds(Config.WAIT_TIME_THRESHOLD_IN_SECONDS));

            WebElement dropdownList = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("pageSize-list")));

            List<WebElement> options = dropdownList.findElements(By.tagName("button"));

            int selectedValue = 0;

            // the smallest dropdown value greater than numOfItemsJSON

            for (WebElement option : options) {
                int value = Integer.parseInt(option.getText());
                if (numOfItemsJSON < value) {
                    option.click();
                    selectedValue = value;
                    break;
                }
            }

            wait.until(ExpectedConditions.invisibilityOfElementLocated(By.id("pageSize-list")));

            Common.logDebug("Page: " + getPageName() + ": Dropdown selected: " + selectedValue);

            Common.driver.navigate().refresh();
            Common.sleep();
        } catch (Exception ex) {
            Common.logException("Failure: " + getPageName() + ": Error in clicking dropdown: " + ex.getMessage(), ex);
        }
    }

    private String getSelectedDropdownValue() {
        try {
            WebElement dropdown = Common.waitForElement(By.id("pageSize"));
            return dropdown.getText().trim();
        } catch (Exception ex) {
            Common.logException("Failure: " + getPageName() + ": Error in getting dropdown value: " + ex.getMessage(), ex);
        }

        return "";
    }

    protected WebElement getSaveButtonWebElementDetailsPage() {
        return Common.waitForElement(By.xpath("//button//*[text()='" + getSaveButtonDetailsPage() + "']/../../.."));
    }

    private void testForAllText() {
        Common.logDebug("Tests started for: " + getPageName() + " page: Testing Text");
        for (String text : getColumnNames()) {
            Common.checkTextPresent(text, getPageName());
        }
    }
}

