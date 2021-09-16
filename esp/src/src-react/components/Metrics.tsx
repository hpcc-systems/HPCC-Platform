import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, IIconProps, SearchBox } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { Table } from "@hpcc-js/dgrid";
import nlsHPCC from "src/nlsHPCC";
import { WUTimelinePatched } from "src/Timings";
import { useMetricsOptions, useWorkunitMetrics } from "../hooks/metrics";
import { HolyGrail } from "../layouts/HolyGrail";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { DockPanel, DockPanelItems, ReactWidget } from "../layouts/DockPanel";
import { IScope, MetricGraph, MetricGraphWidget } from "../util/metricGraph";
import { ShortVerticalDivider } from "./Common";
import { MetricsOptions } from "./MetricsOptions";

const filterIcon: IIconProps = { iconName: "Filter" };

const defaultUIState = {
    hasSelection: false
};

const emptyFilter = {};

interface MetricsProps {
    wuid: string;
    filter?: object;
}

export const Metrics: React.FunctionComponent<MetricsProps> = ({
    wuid,
    filter = emptyFilter
}) => {
    const [_uiState, _setUIState] = React.useState({ ...defaultUIState });
    const [timelineFilter, setTimelineFilter] = React.useState("");
    const [selectedMetrics, setSelectedMetrics] = React.useState([]);
    const [metrics, _columns, _activities, _properties, _measures, _scopeTypes] = useWorkunitMetrics(wuid);
    const [showMetricOptions, setShowMetricOptions] = React.useState(false);
    const [options] = useMetricsOptions();

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => { }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "options", text: nlsHPCC.Options, iconProps: { iconName: "Settings" },
            onClick: () => {
                setShowMetricOptions(true);
            }
        }
    ], []);

    const rightButtons = React.useMemo((): ICommandBarItemProps[] => [
    ], []);

    //  Timeline ---
    const timeline = useConst(() => new WUTimelinePatched()
        .maxZoom(Number.MAX_SAFE_INTEGER)
        .baseUrl("")
        .request({
            ScopeFilter: {
                MaxDepth: 3,
                ScopeTypes: []
            },
            NestedFilter: {
                Depth: 0,
                ScopeTypes: []
            },
            PropertiesToReturn: {
                AllProperties: false,
                AllStatistics: true,
                AllHints: false,
                Properties: ["WhenStarted", "TimeElapsed"]
            },
            ScopeOptions: {
                IncludeId: true,
                IncludeScope: true,
                IncludeScopeType: true
            },
            PropertyOptions: {
                IncludeName: true,
                IncludeRawValue: true,
                IncludeFormatted: true,
                IncludeMeasure: true,
                IncludeCreator: true,
                IncludeCreatorType: false
            }
        })
        .on("click", (row, col, sel) => {
            setTimelineFilter(sel ? row[7].ScopeName : "");
        })
    );

    React.useEffect(() => {
        timeline.wuid(wuid);
    }, [timeline, wuid]);

    //  Scopes Table  ---
    const hasFilter = Object.keys(filter).length > 0;

    const [scopeFilter, setScopeFilter] = React.useState("");
    const onChangeScopeFilter = React.useCallback((event: React.FormEvent<HTMLInputElement | HTMLTextAreaElement>, newValue?: string) => {
        setScopeFilter(newValue || "");
    }, []);

    const scopeFilterFunc = React.useCallback((row: object): boolean => {
        const filter = scopeFilter.trim();
        if (filter) {
            let field = "";
            const colonIdx = filter.indexOf(":");
            if (colonIdx > 0) {
                field = filter.substring(0, colonIdx);
            }
            if (field) {
                return row[field]?.indexOf && row[field]?.indexOf(filter.substring(colonIdx + 1)) >= 0;
            }
            for (const key in row) {
                if (row[key]?.indexOf && row[key]?.indexOf(filter) >= 0) {
                    return true;
                }
            }
            return false;
        }
        return true;
    }, [scopeFilter]);

    const scopesTable = useConst(() => new Table()
        .multiSelect(true)
        .columns(["##", nlsHPCC.Type, nlsHPCC.Scope, ...options.properties])
        .sortable(true)
        .on("click", (row, col, sel) => {
            const selection = scopesTable.selection();
            setSelectedMetrics(selection.map(row => row.__lparam));
        })
    );

    React.useEffect(() => {
        scopesTable
            .columns(["##", nlsHPCC.Type, nlsHPCC.Scope, ...options.properties])
            .data(metrics.filter(scopeFilterFunc).filter(row => {
                return (timelineFilter === "" || row.name?.indexOf(timelineFilter) === 0) &&
                    (options.scopeTypes.indexOf(row.type) >= 0);
            }).map((row, idx) => {
                row.__hpcc_id = row.name;
                return [idx, row.type, row.name, ...options.properties.map(p => row[p] !== undefined ? row[p] : ""), row];
            }))
            .lazyRender()
            ;
    }, [hasFilter, metrics, scopesTable, timelineFilter, filter, options.properties, options.scopeTypes, scopeFilterFunc]);

    //  Graph  ---
    const metricGraph = useConst(() => new MetricGraph());
    const metricGraphWidget = useConst(() => new MetricGraphWidget()
        .zoomToFitLimit(1)
        .on("selectionChanged", () => {
            const items = metricGraphWidget.selection().map(id => {
                return metricGraph.item(id);
            });
            // const item = graph.item(row.id);
            updatePropsTable(items);
            updatePropsTable2(items);
            const tableItems = scopesTable.data().filter(tableRow => items.indexOf(tableRow[tableRow.length - 1]) >= 0);
            scopesTable.selection(tableItems);
        })
    );

    React.useEffect(() => {
        metricGraph.load(metrics);
    }, [metrics, metricGraph]);

    const updateMetricGraph = React.useCallback((selection: IScope[]) => {
        if (selection.length) {
            //  Check if selection is already visible  ---
            if (selection.every(row => metricGraphWidget.exists(row.name))) {
                metricGraphWidget
                    .selection(selection.map(s => s.name))
                    .render(() => {
                        metricGraphWidget.zoomToSelection();
                    })
                    ;
            } else {
                metricGraphWidget
                    .dot(metricGraph.graphTpl(selection, options))
                    .resize()
                    .render(() => {
                        metricGraphWidget.selection(selection.map(s => s.name));
                    })
                    ;
            }
        }
    }, [metricGraph, metricGraphWidget, options]);

    const graphButtons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "reset", text: nlsHPCC.Reset, iconProps: { iconName: "Undo" },
            onClick: () => {
                metricGraphWidget.reset();
                updateMetricGraph(selectedMetrics);
            }
        }
    ], [metricGraphWidget, selectedMetrics, updateMetricGraph]);

    const graphRightButtons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "toSel", title: nlsHPCC.ZoomSelection, iconProps: { iconName: "FitPage" },
            onClick: () => metricGraphWidget.zoomToSelection()
        }, {
            key: "tofit", title: nlsHPCC.ZoomAll, iconProps: { iconName: "ScaleVolume" },
            onClick: () => metricGraphWidget.zoomToFit()
        }, {
            key: "tofitWidth", title: nlsHPCC.ZoomWidth, iconProps: { iconName: "FitWidth" },
            onClick: () => metricGraphWidget.zoomToWidth()
        }, {
            key: "100%", title: nlsHPCC.Zoom100Pct, iconProps: { iconName: "ZoomToFit" },
            onClick: () => metricGraphWidget.zoomToScale(1)
        }, {
            key: "plus", title: nlsHPCC.ZoomPlus, iconProps: { iconName: "ZoomIn" },
            onClick: () => metricGraphWidget.zoomPlus()
        }, {
            key: "minus", title: nlsHPCC.ZoomMinus, iconProps: { iconName: "ZoomOut" },
            onClick: () => metricGraphWidget.zoomMinus()
        }

        // { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
    ], [metricGraphWidget]);

    //  Props Table  ---
    const propsTable = useConst(() => new Table()
        .id("propsTable")
        .columns([nlsHPCC.Property, nlsHPCC.Value])
        .columnWidth("none")
    );

    const updatePropsTable = React.useCallback(items => {
        const props = [];
        items.forEach((item, idx) => {
            for (const key in item) {
                if (key.indexOf("__") !== 0) {
                    props.push([key, item[key]]);
                }
            }
            if (idx < items.length - 1) {
                props.push(["------------------------------", "------------------------------"]);
            }
        });
        propsTable
            ?.data(props)
            ?.lazyRender()
            ;
    }, [propsTable]);

    const propsTable2 = useConst(() => new Table()
        .id("propsTable2")
        .columns([nlsHPCC.Property, nlsHPCC.Value])
        .columnWidth("auto")
    );

    const updatePropsTable2 = React.useCallback(items => {
        const columns = [];
        const props = [];
        items.forEach(item => {
            for (const key in item) {
                if (key.indexOf("__") !== 0 && columns.indexOf(key) < 0) {
                    columns.push(key);
                }
            }
        });
        items.forEach(item => {
            const row = [];
            columns.forEach(column => {
                row.push(item[column]);
            });
            props.push(row);
        });
        propsTable2
            ?.columns(columns)
            ?.data(props)
            ?.lazyRender()
            ;
    }, [propsTable2]);

    const portal = useConst(() => new ReactWidget()
        .id("portal")
    );

    React.useEffect(() => {
        portal.children(<h1>{timelineFilter}</h1>).lazyRender();
    }, [portal, timelineFilter]);

    React.useEffect(() => {
        if (selectedMetrics.length) {
            updatePropsTable(selectedMetrics);
            updatePropsTable2(selectedMetrics);
            updateMetricGraph(selectedMetrics);
        }
    }, [selectedMetrics, updateMetricGraph, updatePropsTable, updatePropsTable2]);

    const items: DockPanelItems = React.useMemo<DockPanelItems>((): DockPanelItems => [
        {
            key: "scopesTable",
            title: nlsHPCC.Metrics,
            component: <HolyGrail
                header={<SearchBox value={scopeFilter} onChange={onChangeScopeFilter} iconProps={filterIcon} placeholder={nlsHPCC.Filter} />}
                main={<AutosizeHpccJSComponent widget={scopesTable} ></AutosizeHpccJSComponent>}
            />
        },
        {
            key: "metricGraph",
            title: nlsHPCC.Graph,
            component: <HolyGrail
                header={<CommandBar items={graphButtons} farItems={graphRightButtons} />}
                main={<AutosizeHpccJSComponent widget={metricGraphWidget} ></AutosizeHpccJSComponent>}
            />,
            location: "split-right",
            ref: "scopesTable"
        },
        {
            title: nlsHPCC.Properties,
            widget: propsTable,
            location: "split-bottom",
            ref: "scopesTable"
        },
        {
            title: nlsHPCC.CrossTab,
            widget: propsTable2,
            location: "tab-after",
            ref: propsTable.id()
        }
    ], [graphButtons, graphRightButtons, metricGraphWidget, onChangeScopeFilter, propsTable, propsTable2, scopeFilter, scopesTable]);

    return <HolyGrail
        header={<>
            <CommandBar items={buttons} overflowButtonProps={{}} farItems={rightButtons} />
            <AutosizeHpccJSComponent widget={timeline} fixedHeight={"160px"} padding={4} />
        </>}
        main={
            <>
                <DockPanel storeID="metrics-layout" items={items}></DockPanel>
                <MetricsOptions show={showMetricOptions} setShow={setShowMetricOptions} layout={undefined/*dockPanel?.layout()*/} />
            </>}
    />;
};
