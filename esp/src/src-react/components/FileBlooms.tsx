import * as React from "react";
import { CommandBar, ICommandBarItemProps } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { useFile } from "../hooks/file";
import { HolyGrail } from "../layouts/HolyGrail";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";

interface FileBloomsProps {
    cluster?: string;
    logicalFile: string;
    sort?: QuerySortItem;
}

const defaultSort = { attribute: "FieldNames", descending: false };

export const FileBlooms: React.FunctionComponent<FileBloomsProps> = ({
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
            FieldNames: { label: nlsHPCC.FieldNames, sortable: true, width: 320 },
            Limit: { label: nlsHPCC.Limit, sortable: true, width: 180 },
            Probability: { label: nlsHPCC.Probability, sortable: true, width: 180 },
        };
    }, []);

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

    const copyButtons = useCopyButtons(columns, selection, "fileBlooms");

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <FluentGrid
                data={data}
                primaryID={"FieldNames"}
                sort={sort}
                columns={columns}
                setSelection={setSelection}
                setTotal={setTotal}
                refresh={refreshTable}
            ></FluentGrid>
        }
    />;
};