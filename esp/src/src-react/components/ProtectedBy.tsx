import * as React from "react";
import { CommandBar, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import nlsHPCC from "src/nlsHPCC";
import { useGrid } from "../hooks/grid";
import { useFile } from "../hooks/file";
import { HolyGrail } from "../layouts/HolyGrail";

interface ProtectedByProps {
    cluster: string;
    logicalFile: string;
}

export const ProtectedBy: React.FunctionComponent<ProtectedByProps> = ({
    cluster,
    logicalFile
}) => {

    const [file, , , refreshData] = useFile(cluster, logicalFile);

    //  Grid ---
    const store = useConst(new Observable(new Memory("Owner")));
    const [Grid, _selection, refreshTable, copyButtons] = useGrid({
        store,
        sort: [{ attribute: "Owner", "descending": false }],
        filename: "protectedBy",
        columns: {
            Owner: { label: nlsHPCC.Owner, sortable: false },
            Modified: { label: nlsHPCC.Modified, sortable: false },
        }
    });

    React.useEffect(() => {
        const results = file?.ProtectList?.DFUFileProtect;

        if (results) {
            store.setData(file?.ProtectList?.DFUFileProtect?.map(row => {
                return {
                    Owner: row.Owner,
                    Modified: row.Modified
                };
            }));
            refreshTable();
        }
    }, [store, file?.ProtectList?.DFUFileProtect, refreshTable]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
    ], [refreshData]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <Grid />
        }
    />;
};