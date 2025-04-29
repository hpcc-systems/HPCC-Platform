import * as React from "react";
import { CommandBar, ICommandBarItemProps } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { useFile } from "../hooks/file";
import { HolyGrail } from "../layouts/HolyGrail";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";

interface ProtectedByProps {
    cluster: string;
    logicalFile: string;
    sort?: QuerySortItem;
}

const defaultSort = { attribute: "Owner", descending: false };

export const ProtectedBy: React.FunctionComponent<ProtectedByProps> = ({
    cluster,
    logicalFile,
    sort = defaultSort
}) => {

    const { file, refreshData } = useFile(cluster, logicalFile);
    const [data, setData] = React.useState<any[]>([]);
    const { selection, setSelection, total, setTotal, refreshTable } = useFluentStoreState({});

    const columns = React.useMemo((): FluentColumns => ({
        Owner: { label: nlsHPCC.Owner, width: 320 },
        Modified: { label: nlsHPCC.Modified, width: 320 },
    }), []);

    React.useEffect(() => {
        const protects = file?.ProtectList?.DFUFileProtect;
        if (protects) {
            const rows = protects.map(({ Owner, Modified }) => ({ Owner, Modified }));
            setData(rows);
            // Count distinct owners
            const distinctCount = new Set(rows.map(r => r.Owner)).size;
            setTotal(distinctCount);
        }
    }, [file?.ProtectList?.DFUFileProtect, setTotal]);

    const buttons = React.useMemo<ICommandBarItemProps[]>(
        () => [
            {
                key: "refresh",
                text: `${nlsHPCC.Refresh} (${nlsHPCC.Users}: ${total})`,
                iconProps: { iconName: "Refresh" },
                onClick: () => refreshData()
            }
        ],
        [refreshData, total]
    );

    const copyButtons = useCopyButtons(columns, selection, "protectedBy");

    return (
        <HolyGrail
            header={<CommandBar items={buttons} farItems={copyButtons} />}
            main={
                <FluentGrid
                    data={data}
                    primaryID="Owner"
                    sort={sort}
                    columns={columns}
                    setSelection={setSelection}
                    setTotal={setTotal}
                    refresh={refreshTable}
                />
            }
        />
    );
};