import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link } from "@fluentui/react";
import { makeStyles } from "@fluentui/react-components";
import { SizeMe } from "../layouts/SizeMe";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { useWorkunitResults } from "../hooks/workunit";
import { HolyGrail } from "../layouts/HolyGrail";
import { pivotItemStyle } from "../layouts/pivot";
import { hashHistory } from "../util/history";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { ShortVerticalDivider } from "./Common";
import { Result } from "./Result";
import { OverflowTabList, TabInfo } from "./controls/TabbedPanes/index";

const defaultUIState = {
    hasSelection: false
};

const useStyles = makeStyles({
    container: {
        height: "100%",
        position: "relative"
    }
});

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
                    return <Link href={`#/workunits/${row.Wuid}/outputs/${Name}${hashHistory.location.search}`}>{Name}</Link>;
                }
            },
            FileName: {
                label: nlsHPCC.FileName, sortable: true,
                formatter: (FileName, row) => {
                    return <Link href={`#/files/${FileName}${hashHistory.location.search}`}>{FileName}</Link>;
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
                    window.location.href = `#/workunits/${wuid}/outputs/${selection[0].Name}${hashHistory.location.search}`;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(`#/workunits/${wuid}/outputs/${selection[i].Name}${hashHistory.location.search}`, "_blank");
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

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
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
        }
    />;
};

interface TabbedResultsProps {
    wuid: string;
    filter?: { [id: string]: any };
}

export const TabbedResults: React.FunctionComponent<TabbedResultsProps> = ({
    wuid,
    filter = {}
}) => {

    const [results] = useWorkunitResults(wuid);

    const tabs = React.useMemo((): TabInfo[] => {
        return results.map(result => ({
            id: `${result?.Sequence ?? ""}`,
            label: result?.ResultName ?? ""
        }));
    }, [results]);

    const [selectedTab, setSelectedTab] = React.useState<string>("");

    React.useEffect(() => {
        const firstTab = tabs[0]?.id ?? "";
        if (!selectedTab || !tabs.some(tab => tab.id === selectedTab)) {
            setSelectedTab(firstTab);
        }
    }, [selectedTab, tabs]);

    const onTabSelect = React.useCallback((tab: TabInfo) => {
        setSelectedTab(tab.id);
    }, []);

    const selectedResult = React.useMemo(() => {
        return results.find(result => `${result?.Sequence ?? ""}` === selectedTab) ?? results[0];
    }, [results, selectedTab]);

    const styles = useStyles();

    return <SizeMe>{({ size }) =>
        <div className={styles.container}>
            <OverflowTabList tabs={tabs} selected={selectedTab} onTabSelect={onTabSelect} size="medium" />
            {selectedResult &&
                <div style={pivotItemStyle(size)}>
                    <Result wuid={wuid} resultName={selectedResult?.ResultName} filter={filter} />
                </div>
            }
        </div>
    }</SizeMe>;

};