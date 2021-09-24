import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { scopedLogger } from "@hpcc-js/util";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import nlsHPCC from "src/nlsHPCC";
import { useGrid } from "../hooks/grid";
import { useFile } from "../hooks/file";
import { HolyGrail } from "../layouts/HolyGrail";
import * as WsDfu from "../../src/WsDfu";
import { ShortVerticalDivider } from "./Common";

const logger = scopedLogger("../components/FileHistory.tsx");

interface FileHistoryProps {
    cluster: string;
    logicalFile: string;
}

export const FileHistory: React.FunctionComponent<FileHistoryProps> = ({
    cluster,
    logicalFile
}) => {

    const [file, , _refresh] = useFile(cluster, logicalFile);

    //  Command Bar  ---
    const buttons: ICommandBarItemProps[] = [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshTable()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "erase", text: nlsHPCC.EraseHistory,
            onClick: () => {
                if (confirm(nlsHPCC.EraseHistoryQ + "\n" + file?.Name + "?")) {
                    WsDfu.EraseHistory({
                        request: {
                            Name: file?.Name
                        }
                    })
                        .then(response => {
                            if (response) {
                                store.setData([]);
                                refreshTable();
                            }
                        })
                        .catch(logger.error)
                        ;
                }
            }
        },
    ];

    //  Grid ---
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

    React.useEffect(() => {
        WsDfu.ListHistory({
            request: {
                Name: file?.Name
            }
        }).then(response => {
            const results = response?.ListHistoryResponse?.History?.Origin;

            if (results) {
                store.setData(results.map(row => {
                    return {
                        Name: row.Name,
                        IP: row.IP,
                        Operation: row.Operation,
                        Owner: row.Owner,
                        Path: row.Path,
                        Timestamp: row.Timestamp,
                        Workunit: row.Workunit
                    };
                }));
                refreshTable();
            }
        }).catch(logger.error)
            ;
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [store, file?.Name]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            < Grid />
        }
    />;
};