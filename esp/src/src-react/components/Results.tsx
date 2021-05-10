import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { AlphaNumSortMemory } from "src/Memory";
import * as Observable from "dojo/store/Observable";
import nlsHPCC from "src/nlsHPCC";
import { useWorkunitResults } from "../hooks/Workunit";
import { HolyGrail } from "../layouts/HolyGrail";
import { createCopyDownloadSelection, ShortVerticalDivider } from "./Common";
import { DojoGrid, selector } from "./DojoGrid";

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
        {
            key: "open", text: nlsHPCC.Open, disabled: !uiState.hasSelection, iconProps: { iconName: "WindowEdit" },
            onClick: () => {
                if (selection.length === 1) {
                    window.location.href = `#/workunits/${wuid}/outputs/${selection[0].Name}`;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(`#/workunits/${wuid}/outputs/${selection[i].Name}`, "_blank");
                    }
                }
            }
        },
        {
            key: "open legacy", text: nlsHPCC.OpenLegacyMode, disabled: !uiState.hasSelection, iconProps: { iconName: "WindowEdit" },
            onClick: () => {
                if (selection.length === 1) {
                    window.location.href = `#/workunits/${wuid}/outputs/${selection[0].Name}/legacy`;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(`#/workunits/${wuid}/outputs/${selection[i].Name}/legacy`, "_blank");
                    }
                }
            }
        },
    ];

    const rightButtons: ICommandBarItemProps[] = [
        ...createCopyDownloadSelection(grid, selection, "results.csv")
    ];

    //  Grid ---
    const gridStore = useConst(new Observable(new AlphaNumSortMemory("__hpcc_id", { Name: true, Value: true })));
    const gridQuery = useConst({});
    const gridSort = useConst([{ attribute: "Wuid", "descending": true }]);
    const gridColumns = useConst({
        col1: selector({
            width: 27,
            selectorType: "checkbox"
        }),
        Name: {
            label: nlsHPCC.Name, width: 180, sortable: true,
            formatter: function (Name, row) {
                return `<a href='#/workunits/${wuid}/outputs/${Name}' class='dgrid-row-url'>${Name}</a>`;
            }
        },
        FileName: {
            label: nlsHPCC.FileName, sortable: true,
            formatter: function (FileName, idx) {
                return `<a href='#/files/${FileName}' class='dgrid-row-url2'>${FileName}</a>`;
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
        grid?.set("query", gridQuery);
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
            const tmp: any = row?.ResultViews;
            return {
                __hpcc_id: row.Name,
                Name: row.Name,
                FileName: row.FileName,
                Value: row.Value,
                ResultViews: tmp?.View,
                Sequence: row.Sequence
            };
        }));
        refreshTable();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [gridStore, results]);

    return <HolyGrail
        header={<CommandBar items={buttons} overflowButtonProps={{}} farItems={rightButtons} />}
        main={
            <DojoGrid store={gridStore} query={gridQuery} sort={gridSort} columns={gridColumns} setGrid={setGrid} setSelection={setSelection} />
        }
    />;
};
