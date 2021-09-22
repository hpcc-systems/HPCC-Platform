import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { scopedLogger } from "@hpcc-js/util";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import nlsHPCC from "src/nlsHPCC";
import { useGrid } from "../hooks/grid";
import { useFile } from "../hooks/file";
import { HolyGrail } from "../layouts/HolyGrail";
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

    //  Grid ---
    const store = useConst(new Observable(new Memory("Owner")));
    const [Grid, _selection, refreshTable, _copyButtons] = useGrid({
        store,
        sort: [{ attribute: "Owner", "descending": false }],
        filename: "protectedBy",
        columns: {
            Owner: { label: nlsHPCC.Owner, sortable: false },
            Modified: { label: nlsHPCC.Modified, sortable: false },
        }
    });

    React.useEffect(() => {
        WsDfu.DFUInfo({
            request: {
                Name: file?.Name
            }
        }).then(response => {
            const results = response?.DFUInfoResponse?.FileDetail.ProtectList.DFUFileProtect;

            if (results) {
                store.setData(results.map(row => {
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
    }, [store, file?.Name, refreshTable]);

    return <HolyGrail
        main={
            <Grid />
        }
    />;
};