package framework.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import framework.utility.TimeUtils;

public class ECLWorkunit {

    @JsonProperty("Wuid")
    private String wuid;
    @JsonProperty("Owner")
    private String owner;

    @JsonProperty("Cluster")
    private String cluster;

    @JsonProperty("Jobname")
    private String jobname;

    @JsonProperty("StateID")
    private int stateID;

    @JsonProperty("State")
    private String state;

    @JsonProperty("Protected")
    private boolean isProtected;

    @JsonProperty("Action")
    private int action;

    @JsonProperty("ActionEx")
    private String actionEx;

    @JsonProperty("IsPausing")
    private boolean isPausing;

    @JsonProperty("ThorLCR")
    private boolean thorLCR;

    @JsonProperty("TotalClusterTime")
    private long totalClusterTime;

    @JsonProperty("ApplicationValues")
    private ApplicationValues applicationValues;

    @JsonProperty("ExecuteCost")
    private double executeCost;

    @JsonProperty("FileAccessCost")
    private double fileAccessCost;

    @JsonProperty("CompileCost")
    private double compileCost;

    @JsonProperty("NoAccess")
    private boolean noAccess;

    public String getWuid() {
        return wuid;
    }

    public String getOwner() {
        return owner;
    }

    public String getCluster() {
        return cluster;
    }

    public String getJobname() {
        return jobname;
    }

    public String getState() {
        return state;
    }

    public long getTotalClusterTime() {
        return totalClusterTime;
    }

    @JsonSetter("TotalClusterTime")
    public void setTotalClusterTime(String totalClusterTime) {
        this.totalClusterTime = TimeUtils.convertToMilliseconds(totalClusterTime);
    }

    public double getExecuteCost() {
        return executeCost;
    }

    public double getFileAccessCost() {
        return fileAccessCost;
    }

    public double getCompileCost() {
        return compileCost;
    }

}
