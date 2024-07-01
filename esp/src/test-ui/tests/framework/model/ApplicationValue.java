package framework.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ApplicationValue {

    @JsonProperty("Application")
    private String application;

    @JsonProperty("Name")
    private String name;

    @JsonProperty("Value")
    private String value;
}
