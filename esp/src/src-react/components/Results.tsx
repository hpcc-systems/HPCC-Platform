import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link, ScrollablePane, Sticky } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { AlphaNumSortMemory } from "src/Memory";
import nlsHPCC from "src/nlsHPCC";
import { useFluentGrid } from "../hooks/grid";
import { useWorkunitResults } from "../hooks/workunit";
import { ShortVerticalDivider } from "./Common";
import { selector } from "./DojoGrid";

const defaultUIState = {
    hasSelection: false
};

interface ResultsProps {
    wuid: string;
}

export const Results: React.FunctionComponent<ResultsProps> = ({
    wuid
}) => {

    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [results, , , refreshData] = useWorkunitResults(wuid);

    //  Grid ---
    const store = useConst(new AlphaNumSortMemory("__hpcc_id", { Name: true, Value: true }));
    const [Grid, selection, copyButtons] = useFluentGrid({
        store,
        sort: [{ attribute: "Wuid", "descending": true }],
        filename: "results",
        columns: {
            col1: selector({
                width: 27,
                selectorType: "checkbox"
            }),
            Name: {
                label: nlsHPCC.Name, width: 180, sortable: true,
                formatter: function (Name, row) {
                    return <Link href={`#/workunits/${row.Wuid}/outputs/${Name}`}>{Name}</Link>;
                }
            },
            FileName: {
                label: nlsHPCC.FileName, sortable: true,
                formatter: function (FileName, row) {
                    return <Link href={`#/files/${FileName}`}>{FileName}</Link>;
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
                    return <>
                        {ResultViews?.map((item, idx) => <Link href='#' viewName={encodeURIComponent(item)}>{item}</Link>)}
                    </>;
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
    ], [refreshData, selection, uiState.hasSelection, wuid]);

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
        store.setData(results.map(row => {
            const tmp: any = row.ResultViews;
            return {
                __hpcc_id: row.Name,
                Name: row.Name,
                Wuid: row.Wuid,
                FileName: row.FileName,
                Value: row.Value,
                ResultViews: tmp?.View,
                Sequence: row.Sequence
            };
        }));
    }, [store, results]);

    return <ScrollablePane>
        <Sticky>
            <CommandBar items={buttons} farItems={copyButtons} />
        </Sticky>
        <Grid />
    </ScrollablePane>;
};
