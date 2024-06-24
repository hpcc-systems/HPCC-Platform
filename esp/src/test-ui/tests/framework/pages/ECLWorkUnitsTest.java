package framework.pages;

import com.fasterxml.jackson.databind.ObjectMapper;
import framework.config.Config;
import framework.model.ECLWorkunit;
import framework.model.WUQueryResponse;
import framework.model.WUQueryRoot;
import framework.utility.Common;
import framework.utility.TimeUtils;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    protected String[] getDetailKeys() {
        return new String[]{"wuid", "action", "state", "owner", "jobname", "compileCost", "executeCost", "fileAccessCost", "protected", "cluster", "totalClusterTime"};
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
        return Arrays.asList("Compile Cost", "Execution Cost", "File Access Cost", "compileCost", "executeCost", "fileAccessCost");
    }

    @Override
    protected Map<String, ECLWorkunit> getJsonMap() {
        return jsonMap;
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
    protected Object parseDataUIValue(Object dataUIValue, String columnName, Object dataIDUIValue) {
        try {
            if (getCostColumns().contains(columnName)) {
                dataUIValue = Double.parseDouble(((String) dataUIValue).split(" ")[0]);
            } else if (columnName.equals("Total Cluster Time") || columnName.equals("totalClusterTime")) {
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
            WebElement element = Common.waitForElement(driver, By.id("wuid"));
            if (element != null) {
                return element.getAttribute("title");
            }
        } catch (Exception ex) {
            Common.logError("Error: " + getPageName() + ex.getMessage());
        }
        return "Invalid Page";
    }
}