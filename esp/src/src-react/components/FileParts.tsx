import * as React from "react";
import { ICommandBarItemProps, CommandBar } from "@fluentui/react";
import { format as d3Format } from "@hpcc-js/common";
import nlsHPCC from "src/nlsHPCC";
import { useFluentGrid } from "../hooks/grid";
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

    const [file, , , refreshData] = useFile(cluster, logicalFile);
    const [data, setData] = React.useState<any[]>([]);

    //  Grid ---
    const [Grid, _selection, copyButtons] = useFluentGrid({
        data,
        primaryID: "Id",
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
        const fileParts = file?.fileParts() ?? [];
        setData(fileParts.map(part => {
            return {
                Id: part.Id,
                Copy: part.Copy,
                Ip: part.Ip,
                Cluster: cluster,
                PartsizeInt64: formatNum(part.PartSizeInt64),
                CompressedSize: part.CompressedSize ? formatNum(part.CompressedSize) : ""
            };
        }));
    }, [cluster, file]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        }
    ], [refreshData]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <Grid />
        }
    />;
};