import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { format as d3Format } from "@hpcc-js/common";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import nlsHPCC from "src/nlsHPCC";
import { useGrid } from "../hooks/grid";
import { useFile } from "../hooks/file";
import { HolyGrail } from "../layouts/HolyGrail";

const formatNum = d3Format(",");

interface FilePartsProps {
    cluster?: string;
    logicalFile: string;
}

export const FileParts: React.FunctionComponent<FilePartsProps> = ({
    cluster,
    logicalFile
}) => {

    const [file, , _refresh] = useFile(cluster, logicalFile);

    //  Grid ---
    const store = useConst(new Observable(new Memory("Id")));
    const [Grid, _selection, refreshTable, _copyButtons] = useGrid({
        store,
        sort: [{ attribute: "Id", "descending": false }],
        filename: "fileParts",
        columns: {
            Id: { label: nlsHPCC.Part, sortable: true, },
            Copy: { label: nlsHPCC.Copy, sortable: true, },
            Ip: { label: nlsHPCC.IP, sortable: true, },
            Cluster: { label: nlsHPCC.Cluster, sortable: true, },
            PartsizeInt64: { label: nlsHPCC.Size, sortable: true, },
            CompressedSize: { label: nlsHPCC.CompressedSize, sortable: true, },
        }
    });

    React.useEffect(() => {
        if (file?.DFUFilePartsOnClusters) {
            const fileParts = file?.DFUFilePartsOnClusters?.DFUFilePartsOnCluster[0]?.DFUFileParts?.DFUPart;
            if (fileParts) {
                store.setData(fileParts.map(part => {
                    return {
                        Id: part.Id,
                        Copy: part.Copy,
                        Ip: part.Ip,
                        Cluster: cluster,
                        PartsizeInt64: formatNum(part.PartSizeInt64),
                        CompressedSize: part.CompressedSize ? formatNum(part.CompressedSize) : ""
                    };
                }));
                refreshTable();
            }
        }
    }, [cluster, file?.DFUFilePartsOnClusters, store, refreshTable]);

    return <HolyGrail
        main={
            <Grid />
        }
    />;
};