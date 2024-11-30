import * as React from "react";
import { ICommandBarItemProps, CommandBar } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import * as Utility from "src/Utility";
import { useFile } from "../hooks/file";
import { HolyGrail } from "../layouts/HolyGrail";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";

interface FilePartsProps {
    cluster?: string;
    logicalFile: string;
    sort?: QuerySortItem;
}

const defaultSort = { attribute: "Id", descending: false };

export const FileParts: React.FunctionComponent<FilePartsProps> = ({
    cluster,
    logicalFile,
    sort = defaultSort
}) => {

    const [file, , , refreshData] = useFile(cluster, logicalFile);
    const [data, setData] = React.useState<any[]>([]);
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        return {
            Id: { label: nlsHPCC.Part, sortable: true, width: 80 },
            Copy: { label: nlsHPCC.Copy, sortable: true, width: 80 },
            Ip: { label: nlsHPCC.IP, sortable: true, width: 80 },
            Cluster: { label: nlsHPCC.Cluster, sortable: true, width: 280 },
            PartsizeInt64: {
                label: nlsHPCC.Size, sortable: true, width: 120,
                formatter: (value, row) => {
                    return Utility.safeFormatNum(value);
                }
            },
            CompressedSize: {
                label: nlsHPCC.CompressedSize, sortable: true, width: 120,
                formatter: (value, row) => {
                    return Utility.safeFormatNum(value);
                }
            },
        };
    }, []);

    React.useEffect(() => {
        const fileParts = file?.fileParts() ?? [];
        setData(fileParts.map(part => {
            return {
                Id: part.Id,
                Copy: part.Copy,
                Ip: part.Ip,
                Cluster: cluster,
                PartsizeInt64: part.PartSizeInt64,
                CompressedSize: part.CompressedSize
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

    const copyButtons = useCopyButtons(columns, selection, "fileParts");

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <FluentGrid
                data={data}
                primaryID={"Id"}
                sort={sort}
                columns={columns}
                setSelection={setSelection}
                setTotal={setTotal}
                refresh={refreshTable}
            ></FluentGrid>
        }
    />;
};