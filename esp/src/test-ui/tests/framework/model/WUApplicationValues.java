package framework.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class WUApplicationValues {

    @JsonProperty("ApplicationValue")
    private List<WUApplicationValue> WUApplicationValue;
}
