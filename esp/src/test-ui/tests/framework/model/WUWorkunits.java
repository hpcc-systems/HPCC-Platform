package framework.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class WUWorkunits {

    @JsonProperty("ECLWorkunit")
    private List<WUECLWorkunit> WUECLWorkunit;

    public List<WUECLWorkunit> getECLWorkunit() {
        return WUECLWorkunit;
    }
}
