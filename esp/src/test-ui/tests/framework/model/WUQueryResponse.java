package framework.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class WUQueryResponse {

    @JsonProperty("Type")
    private String type;

    @JsonProperty("LogicalFileSearchType")
    private String logicalFileSearchType;

    @JsonProperty("Count")
    private int count;

    @JsonProperty("PageSize")
    private int pageSize;

    @JsonProperty("NextPage")
    private int nextPage;

    @JsonProperty("LastPage")
    private int lastPage;

    @JsonProperty("NumWUs")
    private int numWUs;

    @JsonProperty("First")
    private boolean first;

    @JsonProperty("PageStartFrom")
    private int pageStartFrom;

    @JsonProperty("PageEndAt")
    private int pageEndAt;

    @JsonProperty("Descending")
    private boolean descending;

    @JsonProperty("BasicQuery")
    private String basicQuery;

    @JsonProperty("Filters")
    private String filters;

    @JsonProperty("CacheHint")
    private long cacheHint;

    @JsonProperty("Workunits")
    private Workunits workunits;

    public Workunits getWorkunits() {
        return workunits;
    }
}
