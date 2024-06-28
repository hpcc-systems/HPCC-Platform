package framework.pages;

import com.fasterxml.jackson.databind.ObjectMapper;
import framework.config.Config;
import framework.model.ECLWorkunit;
import framework.model.WUQueryResponse;
import framework.model.WUQueryRoot;
import framework.utility.Common;
import framework.utility.TimeUtils;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebElement;
import org.testng.annotations.Test;

import java.io.File;
import java.util.*;

// This class is a subclass of BaseTableTest, and it includes test cases for ECL Workunits page and along with the tests of workunits details page.

public class ECLWorkUnitsTest extends BaseTableTest<ECLWorkunit> {

    @Test
    public void testingECLWorkUnitsPage() {
        testPage();
    }

    @Override
    protected String getPageName() {
        return "ECL Workunits";
    }

    @Override
    protected String getPageUrl() {
        return Common.getUrl(Config.ECL_WORK_UNITS_URL);
    }

    @Override
    protected String getJsonFilePath() {
        return Common.isRunningOnLocal() ? Config.PATH_LOCAL_WORKUNITS_JSON : Config.PATH_GH_ACTION_WORKUNITS_JSON;
    }

    @Override
    protected String[] getColumnNames() {
        return new String[]{"WUID", "Owner", "Job Name", "Cluster", "State", "Total Cluster Time", "Compile Cost", "Execution Cost", "File Access Cost"};
    }

    @Override
    protected String[] getColumnKeys() {
        return new String[]{"Wuid", "Owner", "Jobname", "Cluster", "State", "TotalClusterTime", "Compile Cost", "Execution Cost", "File Access Cost"};
    }

    private final List<String> badStates = Arrays.asList("compiled", "failed");

    String saveButtonDetailsPage = "Save";

    @Override
    protected String[] getDetailNames() {
        return new String[]{"WUID", "Action", "State", "Owner", "Job Name", "Description", "Potential Savings", "Compile Cost", "Execution Cost", "File Access Cost", "Protected", "Cluster", "Total Cluster Time", "Aborted by", "Aborted time", "Services"};
    }

    @Override
    protected String[] getDetailKeys() {
        WebElement element = Common.waitForElement(By.id("state"));

        if (badStates.contains(element.getAttribute(getAttributeValueForDetailsPage()))) { // compiled means it's published but not executed - no wu exists for this, check only these columns
            return new String[]{"wuid", "action", "state", "owner", "jobname", "cluster"};
        } else {
            return new String[]{"wuid", "action", "state", "owner", "jobname", "compileCost", "executeCost", "fileAccessCost", "protected", "cluster", "totalClusterTime"};
        }
    }

    @Override
    protected String[] getDetailKeysForPageLoad() {
        return new String[]{"wuid", "state", "jobname", "cluster"};
    }

    @Override
    protected String getCheckboxTypeForDetailsPage() {
        return "checkbox";
    }

    @Override
    protected String getAttributeTypeForDetailsPage() {
        return "type";
    }

    @Override
    protected String getAttributeValueForDetailsPage() {
        return "value";
    }

    @Override
    protected String[] getColumnKeysWithLinks() {
        return new String[]{"Wuid"};
    }

    @Override
    protected String getUniqueKeyName() {
        return "WUID";
    }

    @Override
    protected String getUniqueKey() {
        return "Wuid";
    }

    private final List<String> costColumns = Arrays.asList("Compile Cost", "Execution Cost", "File Access Cost", "compileCost", "executeCost", "fileAccessCost");

    @Override
    protected Map<String, ECLWorkunit> getJsonMap() {
        return jsonMap;
    }

    @Override
    protected void testDetailSpecificFunctionality(String wuName) {

        //testProtectedButtonFunctionality(wuName);
        testDescriptionUpdateFunctionality(wuName);
    }

    private void testDescriptionUpdateFunctionality(String wuName) {

        String newDescription = "Testing Description";

        String oldDescription = updateDescriptionAndSave(newDescription);

        testIfTheDescriptionUpdated(wuName, newDescription);

        Common.logDetail("Reverting Description Update To Old Value: "+ getPageName() +" Details page, for WUID: " + wuName);

        updateDescriptionAndSave(oldDescription);
    }

    private void testIfTheDescriptionUpdated(String wuName, String newDescription) {

        Common.driver.navigate().refresh();

        waitToLoadDetailsPage();

        WebElement element = Common.waitForElement(By.id("description"));

        String updatedDescription = element.getAttribute(getAttributeValueForDetailsPage()).trim();

        if (newDescription.equals(updatedDescription)) {
            Common.logDetail("Success: "+ getPageName() +" Details page. Description Updated Successfully on click of save button: Description After refresh showing on UI: "+ updatedDescription+", Updated description was: "+newDescription+" for WUID: " + wuName);
        }else{
            Common.logError("Failure: "+ getPageName() +" Details page. Description Did Not Update on click of save button : Description After refresh showing on UI: "+ updatedDescription+", Updated description should be: "+newDescription+" for WUID: " + wuName);
        }
    }

    private String updateDescriptionAndSave(String newDescription) {

        waitToLoadDetailsPage();

        WebElement element = Common.waitForElement(By.id("description"));

        String oldDescription = element.getAttribute(getAttributeValueForDetailsPage());

        element.sendKeys(Keys.CONTROL + "a"); // Select all text
        element.sendKeys(Keys.DELETE); // Delete all selected text

        element.sendKeys(newDescription);

        WebElement saveButton = Common.waitForElement(By.xpath("//button//*[text()='" + saveButtonDetailsPage + "']"));

        Common.waitForElementToBeClickable(saveButton);
        saveButton.click();
        Common.sleepWithTime(4); // need to modify this to wait dynamically

        element = Common.waitForElement(By.id("description"));
        Common.logDetail("Old Description: "+ oldDescription +", Updated Description After Save: " + element.getAttribute(getAttributeValueForDetailsPage()));

        return oldDescription;
    }

    private void testProtectedButtonFunctionality(String wuName) {
        try {

            boolean newCheckboxValue = clickProtectedCheckboxAndSave(wuName);

            checkProtectedStatusOnECLWorkunitsPage(wuName, newCheckboxValue);

            WebElement element = Common.waitForElement(By.xpath("//div[text()='" + wuName + "']/.."));
            element.click();

            waitToLoadDetailsPage();

            if (checkIfNewCheckBoxValuePresentInDetailsPage(newCheckboxValue)) {
                Common.logDetail(getPageName() + " Details Page: Reverting the checkbox value for WUID: " + wuName);
                clickProtectedCheckboxAndSave(wuName);
            }

        } catch (Exception ex) {
            Common.logError("Error: ECL WorkUnits: Exception in testing the protected functionality for WUID: " + wuName + ": Error: " + ex.getMessage());
        }
    }

    private boolean checkIfNewCheckBoxValuePresentInDetailsPage(boolean newCheckboxValue) {

        WebElement protectedCheckbox = Common.waitForElement(By.id("protected"));
        boolean currentCheckboxValue = protectedCheckbox.isSelected();

        if (currentCheckboxValue != newCheckboxValue) {
            Common.logError("Failure: " + getPageName() + " Details Page: Testing Protected checkbox value after coming back from ECL Workunits page: Updated value: " + newCheckboxValue + ": Showing: " + currentCheckboxValue + " on Workunits Details page");
            return false;
        } else {
            Common.logDetail("Success: " + getPageName() + " Details Page: Testing Protected checkbox value after coming back from ECL Workunits page: Updated value: " + newCheckboxValue + ": Showing: " + currentCheckboxValue + " on Workunits Details page");
            return true;
        }
    }

    private boolean clickProtectedCheckboxAndSave(String wuName) {

        WebElement protectedCheckbox = Common.waitForElement(By.id("protected"));
        boolean oldCheckboxValue = protectedCheckbox.isSelected();
        WebElement parentElement = protectedCheckbox.findElement(By.xpath(".."));

        JavascriptExecutor js = (JavascriptExecutor) Common.driver;
        js.executeScript("arguments[0].click();", parentElement);

        boolean newCheckboxValue = waitToLoadUpdatedProtectedCheckbox(oldCheckboxValue);

        Common.logDetail("Protected checkbox old Value: " + oldCheckboxValue + ", current value: " + newCheckboxValue + " for WUID: " + wuName);

        WebElement saveButton = Common.waitForElement(By.xpath("//button//*[text()='" + saveButtonDetailsPage + "']"));

        saveButton.click();

        return newCheckboxValue;
    }

    private boolean waitToLoadUpdatedProtectedCheckbox(boolean oldCheckboxValue) {

        int waitTimeInSecs = Config.WAIT_TIME_IN_SECONDS;
        boolean newCheckboxValue;

        do {
            Common.sleepWithTime(waitTimeInSecs);

            WebElement protectedCheckbox = Common.waitForElement(By.id("protected"));
            newCheckboxValue = protectedCheckbox.isSelected();

            if (!Objects.equals(newCheckboxValue, oldCheckboxValue)) {
                break;
            }

            waitTimeInSecs++;

        } while (waitTimeInSecs < Config.WAIT_TIME_THRESHOLD_IN_SECONDS);

        return newCheckboxValue;
    }

    private void checkProtectedStatusOnECLWorkunitsPage(String wuName, boolean newCheckboxValue) {

        Common.driver.navigate().to(getPageUrl());
        Common.driver.navigate().refresh();

        List<Object> columnDataWUID = getDataFromUIUsingColumnKey(getUniqueKey());
        List<Object> columnDataProtected = getDataFromUIUsingColumnKey("Protected");

        for (int i = 1; i < columnDataProtected.size(); i++) { // two web elements are getting fetched for Protected column header

            if (wuName.equals(columnDataWUID.get(i - 1))) {

                String lockStatus = columnDataProtected.get(i) != "" ? "Locked" : "Unlocked";

                if (newCheckboxValue && lockStatus.equals("Locked")) {
                    Common.logDetail("Success: " + getPageName() + " Details Page: Testing Protected checkbox for value: true : Showing Locked on ECL Workunits page");
                } else if (!newCheckboxValue && lockStatus.equals("Unlocked")) {
                    Common.logDetail("Success: " + getPageName() + " Details Page: Testing Protected checkbox for value: false : Showing Unlocked on ECL Workunits page");
                } else {
                    Common.logError("Failure: " + getPageName() + " Details Page: Testing Protected checkbox for value: " + newCheckboxValue + ": Showing: " + lockStatus + " on ECL Workunits page");
                }
            }
        }
    }


    Map<String, ECLWorkunit> jsonMap = new HashMap<>();

    @Override
    protected List<ECLWorkunit> parseJson(String filePath) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        WUQueryRoot wuQueryRoot = objectMapper.readValue(new File(filePath), WUQueryRoot.class);
        WUQueryResponse wuQueryResponse = wuQueryRoot.getWUQueryResponse();
        List<ECLWorkunit> eclWorkunits = wuQueryResponse.getWorkunits().getECLWorkunit();

        for (ECLWorkunit workunit : eclWorkunits) {
            jsonMap.put(workunit.getWuid(), workunit);
        }

        return eclWorkunits;
    }

    @Override
    protected Object getColumnDataFromJson(ECLWorkunit workunit, String columnKey) {
        return switch (columnKey) {
            case "Wuid", "wuid" -> workunit.getWuid();
            case "Owner", "owner" -> workunit.getOwner();
            case "Jobname", "jobname" -> workunit.getJobname();
            case "Cluster", "cluster" -> workunit.getCluster();
            case "State", "state" -> workunit.getState();
            case "TotalClusterTime", "totalClusterTime" -> workunit.getTotalClusterTime();
            case "Compile Cost", "compileCost" -> workunit.getCompileCost();
            case "Execution Cost", "executeCost" -> workunit.getExecuteCost();
            case "File Access Cost", "fileAccessCost" -> workunit.getFileAccessCost();
            case "action" -> workunit.getActionEx();
            case "protected" -> workunit.isProtected();
            default -> null;
        };
    }

    @Override
    protected Object parseDataUIValue(Object dataUIValue, Object dataJSONValue, String columnName, Object dataIDUIValue) {
        try {
            if (costColumns.contains(columnName)) {
                dataUIValue = Double.parseDouble(((String) dataUIValue).split(" ")[0]);
            } else if (columnName.equals("Total Cluster Time") || columnName.equals("totalClusterTime")) {

                if ((long) dataJSONValue == 0 && "".equals(dataUIValue)) {
                    return 0L; // test ok, if Total Cluster Time is 0 in json and blank in UI - Jira raised - HPCC-32147
                }

                long timeInMilliSecs = TimeUtils.convertToMilliseconds((String) dataUIValue);
                if (timeInMilliSecs == Config.MALFORMED_TIME_STRING) {
                    String errMsg = "Failure: " + getPageName() + ": Incorrect time format for " + columnName + " : " + dataUIValue + " in UI for " + getUniqueKeyName() + " : " + dataIDUIValue;
                    Common.logError(errMsg);
                    return dataUIValue;
                }

                return timeInMilliSecs;
            } else if (dataUIValue instanceof String) {
                dataUIValue = ((String) dataUIValue).trim();
            }
        } catch (Exception ex) {
            Common.logError("Failure: " + getPageName() + " Error in parsing UI value: " + dataUIValue + " for column: " + columnName + " ID: " + dataIDUIValue + " Error: " + ex.getMessage());
        }

        return dataUIValue;
    }

    @Override
    protected Object parseDataJSONValue(Object dataJSONValue, String columnName, Object dataIDUIValue) {
        if (columnName.equals("Total Cluster Time") || columnName.equals("totalClusterTime")) {
            long timeInMilliSecs = (long) dataJSONValue;
            if (timeInMilliSecs == Config.MALFORMED_TIME_STRING) {
                String errMsg = "Failure: " + getPageName() + ": Incorrect time format for " + columnName + " in JSON for " + getUniqueKeyName() + " : " + dataIDUIValue;
                Common.logError(errMsg);
                return dataJSONValue;
            }

            return timeInMilliSecs;
        }

        return dataJSONValue;
    }

    @Override
    protected void sortJsonUsingSortOrder(String currentSortOrder, String columnKey) {
        switch (currentSortOrder) {
            case "ascending" -> ascendingSortJson(columnKey);
            case "descending" -> descendingSortJson(columnKey);
            case "none" -> descendingSortJson(getUniqueKey());
        }
    }

    @Override
    protected String getCurrentPage() {
        try {
            WebElement element = Common.waitForElement(By.id("wuid"));
            if (element != null) {
                return element.getAttribute("title");
            }
        } catch (Exception ex) {
            Common.logError("Error: " + getPageName() + ex.getMessage());
        }
        return "Invalid Page";
    }
}