package framework.pages;

import framework.config.Config;
import framework.utility.Common;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

public abstract class BaseTableTest<T> {

    protected abstract String getPageName();

    protected abstract String getPageUrl();

    protected abstract String getJsonFilePath();

    protected abstract String[] getColumnNames();

    protected abstract String[] getColumnKeys();

    protected abstract String getUniqueKeyName();

    protected abstract String getUniqueKey();

    protected abstract String[] getColumnKeysWithLinks();

    protected abstract Object parseDataUIValue(Object dataUIValue, String columnName, Object dataIDUIValue);

    protected abstract Object parseDataJSONValue(Object dataJSONValue, String columnName, Object dataIDUIValue);

    protected abstract List<T> parseJson(String filePath) throws Exception;

    protected abstract Object getColumnDataFromJson(T object, String columnKey);

    protected abstract void sortJsonUsingSortOrder(String currentSortOrder, List<T> jsonObjects, String columnKey);

    protected abstract String getCurrentPage(WebDriver driver);

    protected void testPage() {
        WebDriver driver = Common.driver;
        Common.openWebPage(driver, getPageUrl());

        try {
            Common.logDebug("Tests started for: " + getPageName() + " page.");

            testForAllText(driver);
            testContentAndSortingOrder(driver);
            testLinksInTable(driver);

            Common.logDebug("Tests finished for: " + getPageName() + " page.");

        } catch (Exception ex) {
            Common.logError(ex.getMessage());
        }
    }

    private void testLinksInTable(WebDriver driver) {

        Common.logDebug("Tests started for: " + getPageName() + " page: Testing Links");

        for (String columnKey : getColumnKeysWithLinks()) {

            List<Object> values = getDataFromUIUsingColumnKey(driver, columnKey);

            int i = 1;

            for (Object value : values) {
                try {
                    String name = value.toString().trim();

                    WebElement element = Common.waitForElement(driver, By.xpath("//div[contains(text(), '" + name + "')]/.."));
                    String href = element.findElement(By.tagName("a")).getAttribute("href");

                    String dropdownValueBefore = getSelectedDropdownValue(driver);

                    element.click();

                    if (driver.getPageSource().contains(name)) {
                        String msg = "Success: " + getPageName() + ": Link Test Pass for " + i++ + ". " + name + ". URL : " + href;
                        Common.logDetail(msg);
                    } else {
                        String currentPage = getCurrentPage(driver);
                        String errorMsg = "Failure: " + getPageName() + ": Link Test Fail for " + i++ + ". " + name + " page failed. The current navigation page that we landed on is " + currentPage + ". Current URL : " + href;
                        Common.logError(errorMsg);
                    }

                    driver.navigate().to(getPageUrl());
                    driver.navigate().refresh();

                    String dropdownValueAfter = getSelectedDropdownValue(driver);

                    // Log error if the dropdown value has changed
                    if (!dropdownValueBefore.equals(dropdownValueAfter)) {
                        String dropdownErrorMsg = "Failure: " + getPageName() + ": Dropdown value changed after navigating back. Before: " + dropdownValueBefore + ", After: " + dropdownValueAfter;
                        Common.logError(dropdownErrorMsg);
                    }
                } catch (Exception ex) {
                    Common.logError("Failure: " + getPageName() + ": Exception in testing links for column: " + columnKey + " value: " + value + " Error: " + ex.getMessage());
                }
            }
        }
    }

    private void testContentAndSortingOrder(WebDriver driver) {
        List<T> jsonObjects = getAllObjectsFromJson();

        if (jsonObjects != null) {
            int numOfItemsJSON = jsonObjects.size();
            clickDropdown(driver, numOfItemsJSON);

            Common.logDebug("Tests started for: " + getPageName() + " page: Testing Content");

            if (testTableContent(driver, jsonObjects)) {

                Common.logDebug("Tests started for: " + getPageName() + " page: Testing Sorting Order");

                for (int i = 0; i < getColumnKeys().length; i++) {
                    testTheSortingOrderForOneColumn(driver, jsonObjects, getColumnKeys()[i], getColumnNames()[i]);
                }
            }
        }
    }

    private void testTheSortingOrderForOneColumn(WebDriver driver, List<T> jsonObjects, String columnKey, String columnName) {
        for (int i = 0; i < 3; i++) {

            String currentSortOrder = getCurrentSortingOrder(driver, columnKey);
            if (currentSortOrder != null) {
                List<Object> columnDataFromUI = getDataFromUIUsingColumnKey(driver, columnKey);

                sortJsonUsingSortOrder(currentSortOrder, jsonObjects, columnKey);

                List<Object> columnDataFromJSON = getDataFromJSONUsingColumnKey(columnKey, jsonObjects);
                List<Object> columnDataIDFromUI = getDataFromUIUsingColumnKey(driver, getUniqueKey());

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

    private String getCurrentSortingOrder(WebDriver driver, String columnKey) {

        try {

            WebElement columnHeader = driver.findElement(By.xpath("//*[@*[.='" + columnKey + "']]"));

            columnHeader.click();

            Common.sleep();

            columnHeader = driver.findElement(By.xpath("//*[@*[.='" + columnKey + "']]"));

            //Get the list of attributes for that column header
            String[] attributes = getAttributeList(driver, columnHeader);

            String sortValue = null;

            // Iterate through the list to find the attribute containing "sort"
            for (String attribute : attributes) {
                if (attribute.contains("sort")) {
                    String[] parts = attribute.split("=", 2);
                    if (parts.length == 2) {
                        sortValue = parts[1].replaceAll("['\"]", "");
                        break;
                    }
                }
            }

            return sortValue;

        } catch (Exception ex) {
            Common.logError("Failure: " + getPageName() + " Exception in getting sort order for column: " + columnKey + " Error: " + ex.getMessage());
        }

        return null;
    }

    private String[] getAttributeList(WebDriver driver, WebElement webElement) {
        try {
            JavascriptExecutor executor = (JavascriptExecutor) driver;
            Object attributes = executor.executeScript("var items = {}; for (index = 0; index < arguments[0].attributes.length; ++index) { items[arguments[0].attributes[index].name] = arguments[0].attributes[index].value }; return items;", webElement);
            return attributes.toString().replaceAll("[{}]", "").split(", ");
        } catch (Exception ex) {
            Common.logError("Failure: " + getPageName() + ": Error in getting arguments list for web element: " + webElement.getText() + "Error: " + ex.getMessage());
        }

        return new String[0];
    }

    private List<Object> getDataFromJSONUsingColumnKey(String columnKey, List<T> jsonObjects) {
        List<Object> columnDataFromJSON = new ArrayList<>();
        for (T jsonObject : jsonObjects) {
            columnDataFromJSON.add(getColumnDataFromJson(jsonObject, columnKey));
        }
        return columnDataFromJSON;
    }

    private List<Object> getDataFromUIUsingColumnKey(WebDriver driver, String columnKey) {

        List<Object> columnData = new ArrayList<>();

        try {
            List<WebElement> elements = driver.findElements(By.xpath("//*[@*[.='" + columnKey + "']]"));

            for (int i = 1; i < elements.size(); i++) {
                columnData.add(elements.get(i).getText());
            }
        } catch (Exception ex) {
            Common.logError("Failure: " + getPageName() + ": Error in getting data from UI using column key: " + columnKey + "Error: " + ex.getMessage());
        }

        return columnData;
    }

    void ascendingSortJson(List<T> jsonObjects, String columnKey) {
        try {
            jsonObjects.sort(Comparator.comparing(jsonObject -> (Comparable) getColumnDataFromJson(jsonObject, columnKey)));
        } catch (Exception ex) {
            Common.logError("Failure: " + getPageName() + ": Exception in sorting JSON in ascending order using column key: " + columnKey + " Error: " + ex.getMessage());
        }
    }

    void descendingSortJson(List<T> jsonObjects, String columnKey) {
        try {
            jsonObjects.sort(Comparator.comparing((Function<T, Comparable>) jsonObject -> (Comparable) getColumnDataFromJson(jsonObject, columnKey)).reversed());
        } catch (Exception ex) {
            Common.logError("Failure: " + getPageName() + ": Exception in sorting JSON in descending order using column key: " + columnKey + " Error: " + ex.getMessage());
        }
    }

    private boolean testTableContent(WebDriver driver, List<T> jsonObjects) {
        Common.logDebug("Page: " + getPageName() + ": Number of Objects from Json: " + jsonObjects.size());

        List<Object> columnDataIDFromUI = getDataFromUIUsingColumnKey(driver, getUniqueKey());

        if (jsonObjects.size() != columnDataIDFromUI.size()) {
            Common.sleep();
            columnDataIDFromUI = getDataFromUIUsingColumnKey(driver, getUniqueKey());
        }

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
            List<Object> columnDataFromUI = getDataFromUIUsingColumnKey(driver, getColumnKeys()[i]);
            List<Object> columnDataFromJSON = getDataFromJSONUsingColumnKey(getColumnKeys()[i], jsonObjects);
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
            Common.logError("Failure: " + getPageName() + " Unable to parse JSON File: Exception: " + ex.getMessage());
        }

        return null;
    }

    private boolean compareData(List<Object> dataUI, List<Object> dataJSON, List<Object> dataIDUI, String columnName) {

        boolean pass = true;

        for (int i = 0; i < dataUI.size(); i++) {

            Object dataUIValue = dataUI.get(i);
            Object dataJSONValue = dataJSON.get(i);
            Object dataIDUIValue = dataIDUI.get(i);

            dataUIValue = parseDataUIValue(dataUIValue, columnName, dataIDUIValue);
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

    // need to change and fix "pageSize" and "ms-Dropdown-item"
    private void clickDropdown(WebDriver driver, int numOfItemsJSON) {

        try {
            WebElement dropdown = driver.findElement(By.id("pageSize"));
            dropdown.click();

            WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(10));
            List<WebElement> options = wait.until(ExpectedConditions.visibilityOfAllElementsLocatedBy(By.cssSelector(".ms-Dropdown-item")));

            int selectedValue = Config.dropdownValues[0];

            // the smallest dropdown value greater than numOfItemsJSON
            for (int value : Config.dropdownValues) {
                if (numOfItemsJSON < value) {
                    selectedValue = value;
                    break;
                }
            }

            Common.logDebug("Page: " + getPageName() + ": Dropdown selected: " + selectedValue);

            for (WebElement option : options) {
                if (option.getText().equals(String.valueOf(selectedValue))) {
                    option.click();
                    break;
                }
            }

            wait.until(ExpectedConditions.invisibilityOfElementLocated(By.cssSelector(".ms-Dropdown-item")));
            driver.navigate().refresh();
            Common.sleep();
        } catch (Exception ex) {
            Common.logError("Failure: " + getPageName() + ": Error in clicking dropdown: " + ex.getMessage());
        }
    }

    private String getSelectedDropdownValue(WebDriver driver) {
        try {
            WebElement dropdown = Common.waitForElement(driver, By.id("pageSize"));
            return dropdown.getText().trim();
        } catch (Exception ex) {
            Common.logError("Failure: " + getPageName() + ": Error in getting dropdown value: " + ex.getMessage());
        }

        return "";
    }

    private void testForAllText(WebDriver driver) {
        Common.logDebug("Tests started for: " + getPageName() + " page: Testing Text");
        for (String text : getColumnNames()) {
            Common.checkTextPresent(driver, text, getPageName());
        }
    }
}

