import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { AlphaNumSortMemory } from "src/Memory";
import * as Observable from "dojo/store/Observable";
import nlsHPCC from "src/nlsHPCC";
import { useWorkunitResources } from "../hooks/Workunit";
import { HolyGrail } from "../layouts/HolyGrail";
import { createCopyDownloadSelection, ShortVerticalDivider } from "./Common";
import { DojoGrid, selector } from "./DojoGrid";

const defaultUIState = {
    hasSelection: false
};

interface ResourcesProps {
    wuid: string;
}

export const Resources: React.FunctionComponent<ResourcesProps> = ({
    wuid
}) => {

    const [grid, setGrid] = React.useState<any>(undefined);
    const [selection, setSelection] = React.useState([]);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [resources] = useWorkunitResources(wuid);

    //  Grid ---
    const gridStore = useConst(new Observable(new AlphaNumSortMemory("DisplayPath", { Name: true, Value: true })));
    const gridQuery = useConst({});
    const gridSort = useConst([{ attribute: "Wuid", "descending": true }]);
    const gridColumns = useConst({
        col1: selector({
            width: 27,
            selectorType: "checkbox"
        }),
        DisplayPath: {
            label: nlsHPCC.Name, sortable: true,
            formatter: function (url, row) {
                return `<a href='#/iframe?src=${encodeURIComponent(`WsWorkunits/${row.URL}`)}' class='dgrid-row-url'>${url}</a>`;
            }
        }
    });

    const refreshTable = React.useCallback((clearSelection = false) => {
        grid?.set("query", gridQuery);
        if (clearSelection) {
            grid?.clearSelection();
        }
    }, [grid, gridQuery]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshTable()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "open", text: nlsHPCC.Open, disabled: !uiState.hasSelection, iconProps: { iconName: "WindowEdit" },
            onClick: () => {
                if (selection.length === 1) {
                    window.location.href = `#/iframe?src=${encodeURIComponent(`WsWorkunits/${selection[0].URL}`)}`;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(`#/iframe?src=${encodeURIComponent(`WsWorkunits/${selection[i].URL}`)}`, "_blank");
                    }
                }
            }
        },
        {
            key: "content", text: nlsHPCC.Content, disabled: !uiState.hasSelection, iconProps: { iconName: "WindowEdit" },
            onClick: () => {
                if (selection.length === 1) {
                    window.location.href = `#/text?src=${encodeURIComponent(`WsWorkunits/${selection[0].URL}`)}`;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(`#/text?src=${encodeURIComponent(`WsWorkunits/${selection[i].URL}`)}`, "_blank");
                    }
                }
            }
        },
    ], [refreshTable, selection, uiState.hasSelection]);

    const rightButtons = React.useMemo((): ICommandBarItemProps[] => [
        ...createCopyDownloadSelection(grid, selection, "roxiequeries.csv")
    ], [grid, selection]);

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
        gridStore.setData(resources.filter((row, idx) => idx > 0).map(row => {
            return {
                URL: row,
                DisplayPath: row.substring(`res/${wuid}/`.length)
            };
        }));
        refreshTable();
    }, [gridStore, refreshTable, resources, wuid]);

    return <HolyGrail
        header={<CommandBar items={buttons} overflowButtonProps={{}} farItems={rightButtons} />}
        main={
            <DojoGrid store={gridStore} query={gridQuery} sort={gridSort} columns={gridColumns} setGrid={setGrid} setSelection={setSelection} />
        }
    />;
};
