import * as React from "react";
import { CommandBar, ICommandBarItemProps } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { useFluentGrid } from "../hooks/grid";
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
    const [data, setData] = React.useState<any[]>([]);

    //  Grid ---
    const { Grid, copyButtons } = useFluentGrid({
        data,
        primaryID: "FieldNames",
        sort: { attribute: "FieldNames", descending: false },
        filename: "fileBlooms",
        columns: {
            FieldNames: { label: nlsHPCC.FieldNames, sortable: true, width: 320 },
            Limit: { label: nlsHPCC.Limit, sortable: true, width: 180 },
            Probability: { label: nlsHPCC.Probability, sortable: true, width: 180 },
        }
    });

    React.useEffect(() => {
        const fileBlooms = file?.Blooms?.DFUFileBloom;
        if (fileBlooms) {
            setData(fileBlooms.map(bloom => {
                return {
                    ...bloom,
                    FieldNames: bloom?.FieldNames?.Item[0] || "",
                };
            }));
        }
    }, [file?.Blooms?.DFUFileBloom]);

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