import * as React from "react";
import { CommandBar, ICommandBarItemProps } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { useFluentGrid } from "../hooks/grid";
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
    const [data, setData] = React.useState<any[]>([]);

    //  Grid ---
    const [Grid, _selection, copyButtons] = useFluentGrid({
        data,
        primaryID: "Owner",
        sort: [{ attribute: "Owner", "descending": false }],
        filename: "protectedBy",
        columns: {
            Owner: { label: nlsHPCC.Owner, width: 320 },
            Modified: { label: nlsHPCC.Modified, width: 320 },
        }
    });

    React.useEffect(() => {
        const results = file?.ProtectList?.DFUFileProtect;

        if (results) {
            setData(file?.ProtectList?.DFUFileProtect?.map(row => {
                return {
                    Owner: row.Owner,
                    Modified: row.Modified
                };
            }));
        }
    }, [file?.ProtectList?.DFUFileProtect]);

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