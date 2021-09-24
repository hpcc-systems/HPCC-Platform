import * as React from "react";
import { CommandBar, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import nlsHPCC from "src/nlsHPCC";
import { useGrid } from "../hooks/grid";
import { useFile } from "../hooks/file";
import { HolyGrail } from "../layouts/HolyGrail";

interface FileBloomsProps {
    cluster?: string;
    logicalFile: string;
}

export const FileBlooms: React.FunctionComponent<FileBloomsProps> = ({
    cluster,
    logicalFile
}) => {

    const [file, , , refreshData] = useFile(cluster, logicalFile);

    //  Grid ---
    const store = useConst(new Observable(new Memory("FieldNames")));
    const [Grid, , refreshTable, copyButtons] = useGrid({
        store,
        sort: [{ attribute: "FieldNames", "descending": false }],
        filename: "fileBlooms",
        columns: {
            FieldNames: { label: nlsHPCC.FieldNames, sortable: true, },
            Limit: { label: nlsHPCC.Limit, sortable: true, },
            Probability: { label: nlsHPCC.Probability, sortable: true, }
        }
    });

    React.useEffect(() => {
        const fileBlooms = file?.Blooms?.DFUFileBloom;
        if (fileBlooms) {
            store.setData(fileBlooms.map(bloom => {
                return {
                    ...bloom,
                    FieldNames: bloom?.FieldNames?.Item[0] || "",
                };
            }));
            refreshTable();
        }
    }, [file?.Blooms?.DFUFileBloom, refreshTable, store]);

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