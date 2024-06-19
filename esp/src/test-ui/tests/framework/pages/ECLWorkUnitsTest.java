package framework.pages;

import com.fasterxml.jackson.databind.ObjectMapper;
import framework.config.Config;
import framework.model.ECLWorkunit;
import framework.model.WUQueryResponse;
import framework.model.WUQueryRoot;
import framework.utility.Common;
import framework.utility.TimeUtils;
import org.openqa.selenium.By;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

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

    private List<String> getCostColumns() {
        return Arrays.asList("Compile Cost", "Execution Cost", "File Access Cost");
    }

    @Override
    protected List<ECLWorkunit> parseJson(String filePath) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        WUQueryRoot wuQueryRoot = objectMapper.readValue(new File(filePath), WUQueryRoot.class);
        WUQueryResponse wuQueryResponse = wuQueryRoot.getWUQueryResponse();
        return wuQueryResponse.getWorkunits().getECLWorkunit();
    }

    @Override
    protected Object getColumnDataFromJson(ECLWorkunit workunit, String columnKey) {
        return switch (columnKey) {
            case "Wuid" -> workunit.getWuid();
            case "Owner" -> workunit.getOwner();
            case "Jobname" -> workunit.getJobname();
            case "Cluster" -> workunit.getCluster();
            case "State" -> workunit.getState();
            case "TotalClusterTime" -> workunit.getTotalClusterTime();
            case "Compile Cost" -> workunit.getCompileCost();
            case "Execution Cost" -> workunit.getExecuteCost();
            case "File Access Cost" -> workunit.getFileAccessCost();
            default -> null;
        };
    }

    @Override
    protected Object parseDataUIValue(Object dataUIValue, String columnName, Object dataIDUIValue, Logger logger) {
        if (getCostColumns().contains(columnName)) {
            dataUIValue = Double.parseDouble(((String) dataUIValue).split(" ")[0]);
        } else if (columnName.equals("Total Cluster Time")) {
            long timeInMilliSecs = TimeUtils.convertToMilliseconds((String) dataUIValue);
            if (timeInMilliSecs == Config.MALFORMED_TIME_STRING) {
                String errMsg = "Failure: " + getPageName() + ": Incorrect time format for " + columnName + " : " + dataUIValue + " in UI for " + getUniqueKeyName() + " : " + dataIDUIValue;
                System.out.println(errMsg);
                logger.severe(errMsg);
                return dataUIValue;
            }

            return timeInMilliSecs;
        } else if (dataUIValue instanceof String) {
            dataUIValue = ((String) dataUIValue).trim();
        }

        return dataUIValue;
    }

    @Override
    protected Object parseDataJSONValue(Object dataJSONValue, String columnName, Object dataIDUIValue, Logger logger) {
        if (columnName.equals("Total Cluster Time")) {
            long timeInMilliSecs = (long) dataJSONValue;
            if (timeInMilliSecs == Config.MALFORMED_TIME_STRING) {
                String errMsg = "Failure: " + getPageName() + ": Incorrect time format for " + columnName + " in JSON for " + getUniqueKeyName() + " : " + dataIDUIValue;
                System.out.println(errMsg);
                logger.severe(errMsg);
                return dataJSONValue;
            }

            return timeInMilliSecs;
        }

        return dataJSONValue;
    }

    @Override
    protected void sortJsonUsingSortOrder(String currentSortOrder, List<ECLWorkunit> workunitsJson, String columnKey) {
        switch (currentSortOrder) {
            case "ascending" -> ascendingSortJson(workunitsJson, columnKey);
            case "descending" -> descendingSortJson(workunitsJson, columnKey);
            case "none" -> descendingSortJson(workunitsJson, getUniqueKey());
        }
    }

    @Override
    protected String getCurrentPage(WebDriver driver) {
        try {
            WebElement element = waitForElement(driver, By.id("wuid"));
            if (element != null) {
                return element.getAttribute("title");
            }
        } catch (NoSuchElementException e) {
            return "Invalid Page";
        }
        return "Invalid Page";
    }
}