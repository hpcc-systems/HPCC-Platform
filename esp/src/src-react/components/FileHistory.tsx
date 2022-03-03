import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { useConfirm } from "../hooks/confirm";
import { useFileHistory } from "../hooks/file";
import { useFluentGrid } from "../hooks/grid";
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
    const [data, setData] = React.useState<any[]>([]);

    const { Grid, copyButtons } = useFluentGrid({
        data,
        primaryID: "__hpcc_id",
        sort: { attribute: "Name", descending: false },
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
        setData(history.map((item, idx) => ({ ...item, __hpcc_id: idx })));
    }, [history]);

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
