import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link, ScrollablePane, Sticky } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { AlphaNumSortMemory } from "src/Memory";
import nlsHPCC from "src/nlsHPCC";
import { useFluentGrid } from "../hooks/grid";
import { useWorkunitResources } from "../hooks/workunit";
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
    const store = useConst(new AlphaNumSortMemory("DisplayPath", { Name: true, Value: true }));
    const [Grid, selection, copyButtons] = useFluentGrid({
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
                    return <Link href={`#/iframe?src=${encodeURIComponent(`WsWorkunits/${row.URL}`)}`}>{url}</Link>;
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
    }, [store, resources, wuid]);

    return <ScrollablePane>
        <Sticky>
            <CommandBar items={buttons} farItems={copyButtons} />
        </Sticky>
        <Grid />
    </ScrollablePane>;
};
