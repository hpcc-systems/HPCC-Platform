import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import nlsHPCC from "src/nlsHPCC";
import { useFile } from "../hooks/File";
import { HolyGrail } from "../layouts/HolyGrail";
import { DojoGrid } from "./DojoGrid";

interface FileBloomsProps {
    cluster?: string;
    logicalFile: string;
}

export const FileBlooms: React.FunctionComponent<FileBloomsProps> = ({
    cluster,
    logicalFile
}) => {

    const [file, , _refresh] = useFile(cluster, logicalFile);
    const [grid, setGrid] = React.useState<any>(undefined);
    const [, setSelection] = React.useState([]);

    //  Grid ---
    const gridStore = useConst(new Observable(new Memory("FieldNames")));
    const gridSort = useConst([{ attribute: "FieldNames", "descending": false }]);
    const gridQuery = useConst({});
    const gridColumns = useConst({
        FieldNames: { label: nlsHPCC.FieldNames, sortable: true, },
        Limit: { label: nlsHPCC.Limit, sortable: true, },
        Probability: { label: nlsHPCC.Probability, sortable: true, },
    });

    const refreshTable = (clearSelection = false) => {
        grid?.set("query", gridQuery);
        if (clearSelection) {
            grid?.clearSelection();
        }
    };

    React.useEffect(() => {
        if (file?.Blooms) {
            const fileBlooms = file?.Blooms?.DFUFileBloom;
            if (fileBlooms) {
                gridStore.setData(fileBlooms.map(bloom => {
                    return {
                        ...bloom,
                        FieldNames: bloom?.FieldNames?.Item[0] || "",
                    };
                }));
                refreshTable();
            }
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [gridStore, file?.Blooms]);

    return <HolyGrail
        main={
            <DojoGrid store={gridStore} query={gridQuery} sort={gridSort} columns={gridColumns} setGrid={setGrid} setSelection={setSelection} />
        }
    />;
};