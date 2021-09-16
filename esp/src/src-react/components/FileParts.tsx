import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { format as d3Format } from "@hpcc-js/common";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import nlsHPCC from "src/nlsHPCC";
import { useFile } from "../hooks/file";
import { HolyGrail } from "../layouts/HolyGrail";
import { DojoGrid } from "./DojoGrid";

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
    const [grid, setGrid] = React.useState<any>(undefined);
    const [, setSelection] = React.useState([]);

    //  Grid ---
    const gridStore = useConst(new Observable(new Memory("Id")));
    const gridSort = useConst([{ attribute: "Id", "descending": false }]);
    const gridQuery = useConst({});
    const gridColumns = useConst({
        Id: { label: nlsHPCC.Part, sortable: true, },
        Copy: { label: nlsHPCC.Copy, sortable: true, },
        Ip: { label: nlsHPCC.IP, sortable: true, },
        Cluster: { label: nlsHPCC.Cluster, sortable: true, },
        PartsizeInt64: { label: nlsHPCC.Size, sortable: true, },
        CompressedSize: { label: nlsHPCC.CompressedSize, sortable: true, },
    });

    const refreshTable = React.useCallback((clearSelection = false) => {
        grid?.set("query", gridQuery);
        if (clearSelection) {
            grid?.clearSelection();
        }
    }, [grid, gridQuery]);

    React.useEffect(() => {
        if (file?.DFUFilePartsOnClusters) {
            const fileParts = file?.DFUFilePartsOnClusters?.DFUFilePartsOnCluster[0]?.DFUFileParts?.DFUPart;
            if (fileParts) {
                gridStore.setData(fileParts.map(part => {
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
    }, [cluster, file?.DFUFilePartsOnClusters, gridStore, refreshTable]);

    return <HolyGrail
        main={
            <DojoGrid store={gridStore} query={gridQuery} sort={gridSort} columns={gridColumns} setGrid={setGrid} setSelection={setSelection} />
        }
    />;
};