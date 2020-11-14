import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import * as Memory from "dojo/store/Memory";
import * as Observable from "dojo/store/Observable";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { useWorkunitResults } from "../hooks/Workunit";
import { HolyGrail } from "../layouts/HolyGrail";
import { ShortVerticalDivider } from "./Common";
import { DojoGrid, selector } from "./DojoGrid";

class MyMemory extends Memory {
    idProperty: "__hpcc_id";
    alphanumSort: { [column: string]: boolean } = {};

    query(query, options) {
        const retVal = super.query(query, options);
        if (options.sort && options.sort.length && this.alphanumSort[options.sort[0].attribute]) {
            Utility.alphanumSort(retVal, options.sort[0].attribute, options.sort[0].descending);
        }
        return retVal;
    }
}

const defaultUIState = {
    hasSelection: false
};

interface ResultsProps {
    wuid: string;
}

export const Results: React.FunctionComponent<ResultsProps> = ({
    wuid
}) => {

    const [grid, setGrid] = React.useState<any>(undefined);
    const [selection, setSelection] = React.useState([]);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [results] = useWorkunitResults(wuid);

    //  Command Bar  ---
    const buttons: ICommandBarItemProps[] = [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshTable()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
    ];

    const rightButtons: ICommandBarItemProps[] = [
        {
            key: "copy", text: nlsHPCC.CopyWUIDs, disabled: !uiState.hasSelection || !navigator?.clipboard?.writeText, iconOnly: true, iconProps: { iconName: "Copy" },
            onClick: () => {
                const wuids = selection.map(s => s.Wuid);
                navigator?.clipboard?.writeText(wuids.join("\n"));
            }
        },
        {
            key: "download", text: nlsHPCC.DownloadToCSV, disabled: !uiState.hasSelection, iconOnly: true, iconProps: { iconName: "Download" },
            onClick: () => {
                Utility.downloadToCSV(grid, selection.map(row => ([row.Protected, row.Wuid, row.Owner, row.Jobname, row.Cluster, row.RoxieCluster, row.State, row.TotalClusterTime])), "workunits.csv");
            }
        }
    ];

    //  Grid ---
    const gridStore = useConst(new Observable(new MyMemory()));
    gridStore.alphanumSort["Name"] = true;
    gridStore.alphanumSort["Value"] = true;

    const gridSort = useConst([{ attribute: "Wuid", "descending": true }]);
    const gridColumns = useConst({
        col1: selector({
            width: 27,
            selectorType: "checkbox"
        }),
        Name: {
            label: nlsHPCC.Name, width: 180, sortable: true,
            formatter: function (Name, idx) {
                return "<a href='#' onClick='return false;' class='dgrid-row-url'>" + Name + "</a>";
            }
        },
        FileName: {
            label: nlsHPCC.FileName, sortable: true,
            formatter: function (FileName, idx) {
                return "<a href='#' onClick='return false;' class='dgrid-row-url2'>" + FileName + "</a>";
            }
        },
        Value: {
            label: nlsHPCC.Value,
            width: 180,
            sortable: true
        },
        ResultViews: {
            label: nlsHPCC.Views, sortable: true,
            formatter: function (ResultViews, idx) {
                let retVal = "";
                ResultViews.forEach((item, idx) => {
                    retVal += "<a href='#' onClick='return false;' viewName=" + encodeURIComponent(item) + " class='dgrid-row-url3'>" + item + "</a>&nbsp;";
                });
                return retVal;
            }
        }
    });

    const refreshTable = (clearSelection = false) => {
        grid?.set("query", {});
        if (clearSelection) {
            grid?.clearSelection();
        }
    };

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        for (let i = 0; i < selection.length; ++i) {
            state.hasSelection = true;
            break;
        }
        setUIState(state);
    }, [selection]);

    React.useEffect(() => {
        gridStore.setData(results.map(row => {
            const tmp: any = row.ResultViews;
            return {
                __hpcc_id: row.Sequence,
                Name: row.Name,
                FileName: row.FileName,
                Value: row.Value,
                ResultViews: tmp.View,
            };
        }));
        refreshTable();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [gridStore, results]);

    return <HolyGrail
        header={<CommandBar items={buttons} overflowButtonProps={{}} farItems={rightButtons} />}
        main={
            <DojoGrid store={gridStore} query={{}} sort={gridSort} columns={gridColumns} setGrid={setGrid} setSelection={setSelection} />
        }
    />;
};
