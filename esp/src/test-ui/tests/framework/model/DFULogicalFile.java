package framework.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DFULogicalFile {

    @JsonProperty("Prefix")
    private String prefix;

    @JsonProperty("NodeGroup")
    private String nodeGroup;

    @JsonProperty("Directory")
    private String directory;

    @JsonProperty("Description")
    private String description;

    @JsonProperty("Parts")
    private String parts;

    @JsonProperty("Name")
    private String name;

    @JsonProperty("Owner")
    private String owner;

    @JsonProperty("Totalsize")
    private String totalSize;

    @JsonProperty("RecordCount")
    private String recordCount;

    @JsonProperty("Modified")
    private String modified;

    @JsonProperty("LongSize")
    private String longSize;

    @JsonProperty("LongRecordCount")
    private String longRecordCount;

    @JsonProperty("isSuperfile")
    private boolean isSuperfile;

    @JsonProperty("isDirectory")
    private boolean isDirectoryBoolean;

    @JsonProperty("Replicate")
    private boolean replicate;

    @JsonProperty("IntSize")
    private int intSize;

    @JsonProperty("IntRecordCount")
    private int intRecordCount;

    @JsonProperty("FromRoxieCluster")
    private String fromRoxieCluster;

    @JsonProperty("BrowseData")
    private boolean browseData;

    @JsonProperty("IsCompressed")
    private boolean isCompressed;

    @JsonProperty("ContentType")
    private String contentType;

    @JsonProperty("CompressedFileSize")
    private int compressedFileSize;

    @JsonProperty("SuperOwners")
    private String superOwners;

    @JsonProperty("Persistent")
    private boolean persistent;

    @JsonProperty("IsProtected")
    private boolean isProtected;

    @JsonProperty("KeyType")
    private String keyType;

    @JsonProperty("NumOfSubfiles")
    private String numOfSubfiles;

    @JsonProperty("Accessed")
    private String accessed;

    @JsonProperty("AtRestCost")
    private double atRestCost;

    @JsonProperty("AccessCost")
    private double accessCost;

    @JsonProperty("MinSkew")
    private int minSkew;

    @JsonProperty("MaxSkew")
    private int maxSkew;

    @JsonProperty("MinSkewPart")
    private int minSkewPart;

    @JsonProperty("MaxSkewPart")
    private int maxSkewPart;

    // Getters and Setters for all fields

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getNodeGroup() {
        return nodeGroup;
    }

    public void setNodeGroup(String nodeGroup) {
        this.nodeGroup = nodeGroup;
    }

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getParts() {
        return parts;
    }

    public void setParts(String parts) {
        this.parts = parts;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getTotalSize() {
        return totalSize;
    }

    public void setTotalSize(String totalSize) {
        this.totalSize = totalSize;
    }

    public String getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(String recordCount) {
        this.recordCount = recordCount;
    }

    public String getModified() {
        return modified;
    }

    public void setModified(String modified) {
        this.modified = modified;
    }

    public String getLongSize() {
        return longSize;
    }

    public void setLongSize(String longSize) {
        this.longSize = longSize;
    }

    public String getLongRecordCount() {
        return longRecordCount;
    }

    public void setLongRecordCount(String longRecordCount) {
        this.longRecordCount = longRecordCount;
    }

    public boolean isSuperfile() {
        return isSuperfile;
    }

    public void setSuperfile(boolean superfile) {
        isSuperfile = superfile;
    }

    public boolean isDirectoryBoolean() {
        return isDirectoryBoolean;
    }

    public void setDirectoryBoolean(boolean isDirectory) {
        isDirectoryBoolean = isDirectory;
    }

    public boolean isReplicate() {
        return replicate;
    }

    public void setReplicate(boolean replicate) {
        this.replicate = replicate;
    }

    public int getIntSize() {
        return intSize;
    }

    public void setIntSize(int intSize) {
        this.intSize = intSize;
    }

    public int getIntRecordCount() {
        return intRecordCount;
    }

    public void setIntRecordCount(int intRecordCount) {
        this.intRecordCount = intRecordCount;
    }

    public String getFromRoxieCluster() {
        return fromRoxieCluster;
    }

    public void setFromRoxieCluster(String fromRoxieCluster) {
        this.fromRoxieCluster = fromRoxieCluster;
    }

    public boolean isBrowseData() {
        return browseData;
    }

    public void setBrowseData(boolean browseData) {
        this.browseData = browseData;
    }

    public boolean isCompressed() {
        return isCompressed;
    }

    public void setCompressed(boolean isCompressed) {
        this.isCompressed = isCompressed;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public int getCompressedFileSize() {
        return compressedFileSize;
    }

    public void setCompressedFileSize(int compressedFileSize) {
        this.compressedFileSize = compressedFileSize;
    }

    public String getSuperOwners() {
        return superOwners;
    }

    public void setSuperOwners(String superOwners) {
        this.superOwners = superOwners;
    }

    public boolean isPersistent() {
        return persistent;
    }

    public void setPersistent(boolean persistent) {
        this.persistent = persistent;
    }

    public boolean isProtected() {
        return isProtected;
    }

    public void setProtected(boolean isProtected) {
        this.isProtected = isProtected;
    }

    public String getKeyType() {
        return keyType;
    }

    public void setKeyType(String keyType) {
        this.keyType = keyType;
    }

    public String getNumOfSubfiles() {
        return numOfSubfiles;
    }

    public void setNumOfSubfiles(String numOfSubfiles) {
        this.numOfSubfiles = numOfSubfiles;
    }

    public String getAccessed() {
        return accessed;
    }

    public void setAccessed(String accessed) {
        this.accessed = accessed;
    }

    public double getAtRestCost() {
        return atRestCost;
    }

    public void setAtRestCost(double atRestCost) {
        this.atRestCost = atRestCost;
    }

    public double getAccessCost() {
        return accessCost;
    }

    public void setAccessCost(double accessCost) {
        this.accessCost = accessCost;
    }

    public int getMinSkew() {
        return minSkew;
    }

    public void setMinSkew(int minSkew) {
        this.minSkew = minSkew;
    }

    public int getMaxSkew() {
        return maxSkew;
    }

    public void setMaxSkew(int maxSkew) {
        this.maxSkew = maxSkew;
    }

    public int getMinSkewPart() {
        return minSkewPart;
    }

    public void setMinSkewPart(int minSkewPart) {
        this.minSkewPart = minSkewPart;
    }

    public int getMaxSkewPart() {
        return maxSkewPart;
    }

    public void setMaxSkewPart(int maxSkewPart) {
        this.maxSkewPart = maxSkewPart;
    }
}
