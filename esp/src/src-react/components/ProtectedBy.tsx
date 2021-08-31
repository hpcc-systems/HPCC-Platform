import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { scopedLogger } from "@hpcc-js/util";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import nlsHPCC from "src/nlsHPCC";
import { useFile } from "../hooks/file";
import { HolyGrail } from "../layouts/HolyGrail";
import { DojoGrid } from "./DojoGrid";
import * as WsDfu from "../../src/WsDfu";

const logger = scopedLogger("../components/ProtectedBy.tsx");

interface ProtectedByProps {
    cluster: string;
    logicalFile: string;
}

export const ProtectedBy: React.FunctionComponent<ProtectedByProps> = ({
    cluster,
    logicalFile
}) => {

    const [file, , _refresh] = useFile(cluster, logicalFile);
    const [grid, setGrid] = React.useState<any>(undefined);
    const [, setSelection] = React.useState([]);

    //  Grid ---
    const gridStore = useConst(new Observable(new Memory("Owner")));
    const gridSort = useConst([{ attribute: "Owner", "descending": false }]);
    const gridQuery = useConst({});
    const gridColumns = useConst({
        Owner: { label: nlsHPCC.Owner, sortable: false },
        Modified: { label: nlsHPCC.Modified, sortable: false },
    });

    const refreshTable = (clearSelection = false) => {
        grid?.set("query", gridQuery);
        if (clearSelection) {
            grid?.clearSelection();
        }
    };

    React.useEffect(() => {
        WsDfu.DFUInfo({
            request: {
                Name: file?.Name
            }
        })
            .then(response => {
                const results = response?.DFUInfoResponse?.FileDetail.ProtectList.DFUFileProtect;

                if (results) {
                    gridStore.setData(results.map(row => {
                        return {
                            Owner: row.Owner,
                            Modified: row.Modified
                        };
                    }));
                    refreshTable();
                }
            })
            .catch(logger.error)
            ;
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [gridStore, file?.Name]);

    return <HolyGrail
        main={
            <DojoGrid store={gridStore} query={gridQuery} sort={gridSort} columns={gridColumns} setGrid={setGrid} setSelection={setSelection} />
        }
    />;
};