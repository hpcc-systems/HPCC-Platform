package framework.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DFUQueryResponse {

    @JsonProperty("DFULogicalFiles")
    private DFULogicalFiles dfuLogicalFiles;

    @JsonProperty("Prefix")
    private String prefix;

    @JsonProperty("NodeGroup")
    private String nodeGroup;

    @JsonProperty("LogicalName")
    private String logicalName;

    @JsonProperty("Description")
    private String description;

    @JsonProperty("Owner")
    private String owner;

    @JsonProperty("StartDate")
    private String startDate;

    @JsonProperty("EndDate")
    private String endDate;

    @JsonProperty("FileType")
    private String fileType;

    @JsonProperty("FileSizeFrom")
    private int fileSizeFrom;

    @JsonProperty("FileSizeTo")
    private int fileSizeTo;

    @JsonProperty("FirstN")
    private int firstN;

    @JsonProperty("PageSize")
    private int pageSize;

    @JsonProperty("PageStartFrom")
    private int pageStartFrom;

    @JsonProperty("LastPageFrom")
    private int lastPageFrom;

    @JsonProperty("PageEndAt")
    private int pageEndAt;

    @JsonProperty("PrevPageFrom")
    private int prevPageFrom;

    @JsonProperty("NextPageFrom")
    private int nextPageFrom;

    @JsonProperty("NumFiles")
    private int numFiles;

    @JsonProperty("Sortby")
    private String sortby;

    @JsonProperty("Descending")
    private boolean descending;

    @JsonProperty("BasicQuery")
    private String basicQuery;

    @JsonProperty("ParametersForPaging")
    private String parametersForPaging;

    @JsonProperty("Filters")
    private String filters;

    @JsonProperty("CacheHint")
    private long cacheHint;

    @JsonProperty("IsSubsetOfFiles")
    private String isSubsetOfFiles;

    @JsonProperty("Warning")
    private String warning;

    public DFULogicalFiles getDFULogicalFiles() {
        return dfuLogicalFiles;
    }

    public void setDFULogicalFiles(DFULogicalFiles dfuLogicalFiles) {
        this.dfuLogicalFiles = dfuLogicalFiles;
    }
}
