import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { AlphaNumSortMemory } from "src/Memory";
import * as Observable from "dojo/store/Observable";
import nlsHPCC from "src/nlsHPCC";
import { useGrid } from "../hooks/grid";
import { useWorkunitResources } from "../hooks/workunit";
import { HolyGrail } from "../layouts/HolyGrail";
import { ShortVerticalDivider } from "./Common";
import { selector } from "./DojoGrid";

const defaultUIState = {
    hasSelection: false
};

interface ResourcesProps {
    wuid: string;
}

export const Resources: React.FunctionComponent<ResourcesProps> = ({
    wuid
}) => {

    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [resources, , , refreshData] = useWorkunitResources(wuid);

    //  Grid ---
    const store = useConst(new Observable(new AlphaNumSortMemory("DisplayPath", { Name: true, Value: true })));
    const [Grid, selection, refreshTable, copyButtons] = useGrid({
        store,
        sort: [{ attribute: "Wuid", "descending": true }],
        filename: "resources",
        columns: {
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
        }
    });

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
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
    ], [refreshData, selection, uiState.hasSelection]);

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
        store.setData(resources.filter((row, idx) => idx > 0).map(row => {
            return {
                URL: row,
                DisplayPath: row.substring(`res/${wuid}/`.length)
            };
        }));
        refreshTable();
    }, [store, refreshTable, resources, wuid]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <Grid />
        }
    />;
};
