import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { Memory, Observable } from "src/Memory";
import nlsHPCC from "src/nlsHPCC";
import { useConfirm } from "../hooks/confirm";
import { useFileHistory } from "../hooks/file";
import { useGrid } from "../hooks/grid";
import { HolyGrail } from "../layouts/HolyGrail";
import { ShortVerticalDivider } from "./Common";

interface FileHistoryProps {
    cluster: string;
    logicalFile: string;
}

export const FileHistory: React.FunctionComponent<FileHistoryProps> = ({
    cluster,
    logicalFile
}) => {

    //  Grid ---
    const [history, eraseHistory, refreshData] = useFileHistory(cluster, logicalFile);

    const store = useConst(new Observable(new Memory("Name")));
    const [Grid, _selection, refreshTable, copyButtons] = useGrid({
        store,
        sort: [{ attribute: "Name", "descending": false }],
        filename: "filehistory",
        columns: {
            Name: { label: nlsHPCC.Name, sortable: false },
            IP: { label: nlsHPCC.IP, sortable: false },
            Operation: { label: nlsHPCC.Operation, sortable: false },
            Owner: { label: nlsHPCC.Owner, sortable: false },
            Path: { label: nlsHPCC.Path, sortable: false },
            Timestamp: { label: nlsHPCC.TimeStamp, sortable: false },
            Workunit: { label: nlsHPCC.Workunit, sortable: false }
        }
    });

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.EraseHistory,
        message: nlsHPCC.EraseHistoryQ + "\n" + logicalFile + "?",
        onSubmit: eraseHistory
    });

    React.useEffect(() => {
        store.setData(history);
        refreshTable();
    }, [history, refreshTable, store]);

    //  Command Bar  ---
    const buttons: ICommandBarItemProps[] = React.useMemo(() => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "erase", text: nlsHPCC.EraseHistory, disabled: history?.length === 0,
            onClick: () => setShowDeleteConfirm(true)
        },
    ], [history?.length, refreshData, setShowDeleteConfirm]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <>
                <Grid />
                <DeleteConfirm />
            </>
        }
    />;
};
