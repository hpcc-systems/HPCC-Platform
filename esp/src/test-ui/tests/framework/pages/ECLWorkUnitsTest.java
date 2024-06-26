package framework.pages;

import com.fasterxml.jackson.databind.ObjectMapper;
import framework.config.Config;
import framework.model.ECLWorkunit;
import framework.model.WUQueryResponse;
import framework.model.WUQueryRoot;
import framework.utility.Common;
import framework.utility.TimeUtils;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    @Override
    protected String[] getDetailNames() {
        return new String[]{"WUID", "Action", "State", "Owner", "Job Name", "Description", "Potential Savings", "Compile Cost", "Execution Cost", "File Access Cost", "Protected", "Cluster", "Total Cluster Time", "Aborted by", "Aborted time", "Services"};
    }

    @Override
    protected String[] getDetailKeys() {
        WebElement element = Common.waitForElement(By.id("state"));

        if (badStates.contains(element.getAttribute(getAttributeTitleForDetailsPage()))) { // compiled means it's published but not executed - no wu exists for this, check only these columns
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
    protected String getAttributeTitleForDetailsPage() {
        return "title";
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