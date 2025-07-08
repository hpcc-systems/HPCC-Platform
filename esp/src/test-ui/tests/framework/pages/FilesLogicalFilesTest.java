package framework.pages;

import com.fasterxml.jackson.databind.ObjectMapper;
import framework.config.Config;
import framework.config.URLConfig;
import framework.model.DFULogicalFile;
import framework.model.DFUQueryResponse;
import framework.model.DFUQueryRoot;
import framework.utility.Common;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// This class is a subclass of BaseTableTest, and it includes test cases for Files LogicalFiles tab and along with the tests of logical files details page.

public class FilesLogicalFilesTest extends BaseTableTest<DFULogicalFile> {

    @Test
    public void testingECLWorkUnitsPage() {
        testPage();
    }

    @Override
    protected String getPageName() {
        return "Files LogicalFiles";
    }

    @Override
    protected String getPageUrl() {
        try {
            return URLConfig.urlMap.get(URLConfig.NAV_FILES).getUrlMappings().get(URLConfig.TAB_FILES_LOGICAL_FILES).getUrl();
        } catch (Exception ex) {
            Common.logException("Error in getting page URL " + getPageName() + ": Exception: " + ex.getMessage(), ex);
        }
        return "";
    }

    @Override
    protected String getJsonFilePath() {
        return Config.PATH_FOLDER_JSON + Config.FILES_JSON_FILE_NAME;
    }

    @Override
    protected String[] getColumnNames() { // these are the names of the columns headers displayed on the UI
        return new String[]{"Logical Name", "Owner", "Super Owner", "Description", "Cluster", "Records", "Size", "Compressed Size", "Parts", "Min Skew", "Max Skew", "Modified (UTC/GMT)", "Last Accessed", "File Cost At Rest", "File Access Cost"};
    }

    @Override
    protected String[] getColumnKeys() { // these are the identifiers used in the HTML code for the respective columns
        return new String[]{"Name", "Owner", "SuperOwners", "Description", "NodeGroup", "Records", "FileSize", "CompressedFileSizeString", "Parts", "MinSkew", "MaxSkew", "Modified", "Accessed", "AtRestCost", "AccessCost"};
    }

    @Override
    protected String getSaveButtonDetailsPage() {
        return "Save";
    }

    @Override
    protected String[] getDetailNames() {
        return new String[]{};
    }

    @Override
    protected String[] getDetailKeys() {
        return new String[]{};
    }

    @Override
    protected String[] getDetailKeysForPageLoad() {
        return new String[]{};
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
        return new String[]{"Name"};
    }

    @Override
    protected String getUniqueKeyName() {
        return "Logical Name";
    }

    @Override
    protected String getUniqueKey() {
        return "Name";
    }

    String sortByColumnKeyWhenSortedByNone = "Modified";

    protected String[] getTabValuesForDetailsPage() {
        return new String[]{};
    }

    @Override
    protected Map<String, List<String>> getColumnNamesForTabsDetailsPage() { // key in this map is the value attribute of the html element of a tab.
        return Map.ofEntries();
    }

    private final List<String> costColumns = Arrays.asList("File Cost At Rest", "File Access Cost", "AtRestCost", "AccessCost");

    @Override
    protected Map<String, DFULogicalFile> getJsonMap() {
        return jsonMap;
    }

    @Override
    protected void testDetailSpecificFunctionality(String wuName, int i) {
    }

    Map<String, DFULogicalFile> jsonMap = new HashMap<>();

    @Override
    protected List<DFULogicalFile> parseJson(String filePath) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        DFUQueryRoot dfuQueryRoot = objectMapper.readValue(new File(filePath), DFUQueryRoot.class);
        DFUQueryResponse dfuQueryResponse = dfuQueryRoot.getDFUQueryResponse();
        List<DFULogicalFile> dfuLogicalFiles = dfuQueryResponse.getDFULogicalFiles().getDFULogicalFile();

        for (DFULogicalFile logicalFile : dfuLogicalFiles) {
            jsonMap.put(logicalFile.getName(), logicalFile);
        }

        return dfuLogicalFiles;
    }

    @Override
    protected Object getColumnDataFromJson(DFULogicalFile logicalFile, String columnKey) {
        return switch (columnKey) {
            case "Name" -> logicalFile.getName();
            case "Owner" -> logicalFile.getOwner();
            case "SuperOwners" -> logicalFile.getSuperOwners();
            case "Description" -> logicalFile.getDescription();
            case "NodeGroup" -> logicalFile.getNodeGroup();
            case "Records" -> logicalFile.getRecordCount();
            case "FileSize" -> logicalFile.getIntSize();
            case "CompressedFileSizeString" -> logicalFile.getCompressedFileSize();
            case "Parts" -> logicalFile.getParts();
            case "MinSkew" -> logicalFile.getMinSkew();
            case "MaxSkew" -> logicalFile.getMaxSkew();
            case "Modified" -> logicalFile.getModified();
            case "Accessed" -> logicalFile.getAccessed();
            case "AtRestCost" -> logicalFile.getAtRestCost();
            case "AccessCost" -> logicalFile.getAccessCost();
            default -> null;
        };
    }

    @Override
    protected Object parseDataUIValue(Object dataUIValue, Object dataJSONValue, String columnName, Object dataIDUIValue) {
        try {
            if (costColumns.contains(columnName)) {
                dataUIValue = Double.parseDouble(((String) dataUIValue).split(" ")[0]);
            } else if (dataUIValue instanceof String) {
                dataUIValue = ((String) dataUIValue).trim();
            }
        } catch (Exception ex) {
            Common.logException("Failure: " + getPageName() + " Error in parsing UI value: " + dataUIValue + " for column: " + columnName + " ID: " + dataIDUIValue + " Error: " + ex.getMessage(), ex);
        }

        return dataUIValue;
    }

    @Override
    protected Object parseDataJSONValue(Object dataJSONValue, String columnName, Object dataIDUIValue) {
        return dataJSONValue;
    }

    @Override
    protected void sortJsonUsingSortOrder(String currentSortOrder, String columnKey) {
        switch (currentSortOrder) {
            case "ascending" -> ascendingSortJson(columnKey);
            case "descending" -> descendingSortJson(columnKey);
            case "none" -> descendingSortJson(sortByColumnKeyWhenSortedByNone);
        }
    }

    @Override
    protected String getCurrentPage() {
        return "Invalid Page";
    }
}
