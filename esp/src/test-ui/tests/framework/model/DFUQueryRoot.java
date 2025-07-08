package framework.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DFUQueryRoot {

    @JsonProperty("DFUQueryResponse")
    private DFUQueryResponse dfuQueryResponse;

    public DFUQueryResponse getDFUQueryResponse() {
        return dfuQueryResponse;
    }

    public void setDFUQueryResponse(DFUQueryResponse dfuQueryResponse) {
        this.dfuQueryResponse = dfuQueryResponse;
    }
}
