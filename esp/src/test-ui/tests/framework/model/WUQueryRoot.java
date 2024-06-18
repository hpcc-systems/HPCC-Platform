package framework.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class WUQueryRoot {
    @JsonProperty("WUQueryResponse")
    private WUQueryResponse wuQueryResponse;

    public WUQueryResponse getWUQueryResponse() {
        return wuQueryResponse;
    }
}
