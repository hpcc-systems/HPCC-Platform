package framework.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ApplicationValues {

    @JsonProperty("ApplicationValue")
    private List<ApplicationValue> applicationValue;
}
