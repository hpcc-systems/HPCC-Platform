package framework.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class DFULogicalFiles {

    @JsonProperty("DFULogicalFile")
    private List<DFULogicalFile> dfuLogicalFile;

    public List<DFULogicalFile> getDFULogicalFile() {
        return dfuLogicalFile;
    }

    public void setDFULogicalFile(List<DFULogicalFile> dfuLogicalFile) {
        this.dfuLogicalFile = dfuLogicalFile;
    }
}
