package framework.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Workunits {

    @JsonProperty("ECLWorkunit")
    private List<ECLWorkunit> eclWorkunit;

    public List<ECLWorkunit> getECLWorkunit() {
        return eclWorkunit;
    }
}
