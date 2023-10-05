import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link, Pivot, PivotItem, ScrollablePane, Sticky } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { useWorkunitResults } from "../hooks/workunit";
import { pivotItemStyle } from "../layouts/pivot";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { ShortVerticalDivider } from "./Common";
import { Result } from "./Result";

const defaultUIState = {
    hasSelection: false
};

interface ResultsProps {
    wuid: string;
    sort?: QuerySortItem;
}

const defaultSort = { attribute: "Wuid", descending: true };

export const Results: React.FunctionComponent<ResultsProps> = ({
    wuid,
    sort = defaultSort
}) => {

    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [results, , , refreshData] = useWorkunitResults(wuid);
    const [data, setData] = React.useState<any[]>([]);
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        return {
            col1: {
                width: 27,
                selectorType: "checkbox"
            },
            Name: {
                label: nlsHPCC.Name, width: 180, sortable: true,
                formatter: (Name, row) => {
                    return <Link href={`#/workunits/${row.Wuid}/outputs/${Name}`}>{Name}</Link>;
                }
            },
            FileName: {
                label: nlsHPCC.FileName, sortable: true,
                formatter: (FileName, row) => {
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
                formatter: (ResultViews, idx) => {
                    return <>
                        {ResultViews?.map((item, idx) => <Link href='#' viewName={encodeURIComponent(item)}>{item}</Link>)}
                    </>;
                }
            }
        };
    }, []);

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
                    window.location.href = `#/workunits/${wuid}/outputs/${selection[0].Name}?__legacy`;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(`#/workunits/${wuid}/outputs/${selection[i].Name}?__legacy`, "_blank");
                    }
                }
            }
        },
        {
            key: "visualize", text: nlsHPCC.Visualize, disabled: !uiState.hasSelection, iconProps: { iconName: "BarChartVertical" },
            onClick: () => {
                if (selection.length === 1) {
                    window.location.href = `#/workunits/${wuid}/outputs/${selection[0].Sequence}?__visualize`;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(`#/workunits/${wuid}/outputs/${selection[i].Sequence}?__visualize`, "_blank");
                    }
                }
            }
        },
    ], [refreshData, selection, uiState.hasSelection, wuid]);

    const copyButtons = useCopyButtons(columns, selection, "results");

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
        setData(results.map(row => {
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
    }, [results]);

    return <ScrollablePane>
        <Sticky>
            <CommandBar items={buttons} farItems={copyButtons} />
        </Sticky>
        <FluentGrid
            data={data}
            primaryID={"__hpcc_id"}
            alphaNumColumns={{ Name: true, Value: true }}
            sort={sort}
            columns={columns}
            setSelection={setSelection}
            setTotal={setTotal}
            refresh={refreshTable}
        ></FluentGrid>
    </ScrollablePane >;
};

interface TabbedResultsProps {
    wuid: string;
}

export const TabbedResults: React.FunctionComponent<TabbedResultsProps> = ({
    wuid
}) => {

    const [results] = useWorkunitResults(wuid);

    return <SizeMe monitorHeight>{({ size }) =>
        <Pivot overflowBehavior="menu" style={{ height: "100%" }}>
            {results.map(result => {
                return <PivotItem key={`${result?.ResultName}_${result?.Sequence}`} headerText={result?.ResultName} style={pivotItemStyle(size)}>
                    <Result wuid={wuid} resultName={result?.ResultName} />
                </PivotItem>;
            })}
        </Pivot>
    }</SizeMe>;

};