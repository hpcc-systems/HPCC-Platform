import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, IIconProps, SearchBox } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
// Disable until ESP supports hotspot
// import { WorkunitsServiceEx } from "@hpcc-js/comms";
import { Table } from "@hpcc-js/dgrid";
import { compare } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { WUTimelinePatched } from "src/Timings";
import { useDeepEffect } from "../hooks/deepHooks";
import { useMetricsOptions, useWorkunitMetrics } from "../hooks/metrics";
import { HolyGrail } from "../layouts/HolyGrail";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { DockPanel, DockPanelItems, ReactWidget, ResetableDockPanel } from "../layouts/DockPanel";
import { IScope, MetricGraph, MetricGraphWidget } from "../util/metricGraph";
import { pushUrl } from "../util/history";
import { debounce } from "../util/throttle";
import { ErrorBoundary } from "../util/errorBoundary";
import { ShortVerticalDivider } from "./Common";
import { MetricsOptions } from "./MetricsOptions";

// Disable until ESP supports hotspot
// const logger = scopedLogger("src-react/components/Metrics.tsx");

const filterIcon: IIconProps = { iconName: "Filter" };

const defaultUIState = {
    hasSelection: false
};

interface MetricsProps {
    wuid: string;
    parentUrl?: string;
    selection?: string;
}

export const Metrics: React.FunctionComponent<MetricsProps> = ({
    wuid,
    parentUrl = `/workunits/${wuid}/metrics`,
    selection = ""
}) => {
    const [_uiState, _setUIState] = React.useState({ ...defaultUIState });
    const [timelineFilter, setTimelineFilter] = React.useState("");
    const [selectedMetrics, setSelectedMetrics] = React.useState([]);
    const [selectedMetricsPtr, setSelectedMetricsPtr] = React.useState<number>(-1);
    const [metrics, _columns, _activities, _properties, _measures, _scopeTypes, refresh] = useWorkunitMetrics(wuid);
    const [showMetricOptions, setShowMetricOptions] = React.useState(false);
    const [options, setOptions, saveOptions] = useMetricsOptions();
    const [dockpanel, setDockpanel] = React.useState<ResetableDockPanel>();
    const [showTimeline, setShowTimeline] = React.useState<boolean>(true);
    const [trackSelection, setTrackSelection] = React.useState<boolean>(true);
    const [fullscreen, setFullscreen] = React.useState<boolean>(false);

    // Disable until ESP supports hotspot
    // const onHotspot = React.useCallback(() => {
    //     const service = new WorkunitsServiceEx({ baseUrl: "" });
    //     service.WUAnalyseHotspot({
    //         Wuid: wuid,
    //         RootScope: "",
    //         OptOnlyActive: false,
    //         OnlyCriticalPath: false,
    //         IncludeProperties: true,
    //         IncludeStatistics: true,
    //         ThresholdPercent: 1.0,
    //         PropertyOptions: {
    //             IncludeName: true,
    //             IncludeRawValue: false,
    //             IncludeFormatted: true,
    //             IncludeMeasure: true,
    //             IncludeCreator: false,
    //             IncludeCreatorType: false
    //         }
    //     }).then(response => {
    //         const selection: string = response.Activities.Activity.map(activity => activity.Id).join(",");
    //         pushUrl(`/workunits/${wuid}/metrics/${selection}`);
    //     }).catch(err => logger.error(err));
    // }, [wuid]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refresh()
        },
        {
            key: "hotspot", text: nlsHPCC.Hotspots, iconProps: { iconName: "SpeedHigh" },
            // Disable until ESP supports hotspot
            disabled: true  // , onClick: () => onHotspot()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "timeline", text: nlsHPCC.Timeline, canCheck: true, checked: showTimeline, iconProps: { iconName: "BarChartHorizontal" },
            onClick: () => {
                setShowTimeline(!showTimeline);
            }
        },
        {
            key: "options", text: nlsHPCC.Options, iconProps: { iconName: "Settings" },
            onClick: () => {
                setOptions({ ...options, layout: dockpanel.layout() });
                setShowMetricOptions(true);
            }
        }
    ], [dockpanel, options, refresh, setOptions, showTimeline]);

    const rightButtons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "fullscreen", title: nlsHPCC.MaximizeRestore, iconProps: { iconName: fullscreen ? "ChromeRestore" : "FullScreen" },
            onClick: () => setFullscreen(!fullscreen)
        }
    ], [fullscreen]);

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
        .on("click", debounce((row, col, sel) => {
            const selection = scopesTable.selection();
            pushUrl(`${parentUrl}/${selection.map(row => row.__lparam.id).join(",")}`);
        }, 100))
    );

    const [tableLoaded, setTableLoaded] = React.useState(false);
    React.useEffect(() => {
        setTableLoaded(false);
        scopesTable
            .columns(["##", nlsHPCC.Type, nlsHPCC.Scope, ...options.properties])
            .data(metrics.filter(scopeFilterFunc).filter(row => {
                return (timelineFilter === "" || row.name?.indexOf(timelineFilter) === 0) &&
                    (options.scopeTypes.indexOf(row.type) >= 0);
            }).map((row, idx) => {
                row.__hpcc_id = row.name;
                return [idx, row.type, row.name, ...options.properties.map(p => row[p] !== undefined ? row[p] : ""), row];
            }))
            .render()
            ;
        setTableLoaded(true);
    }, [metrics, options.properties, options.scopeTypes, scopeFilterFunc, scopesTable, timelineFilter]);

    const updateScopesTable = React.useCallback((selection: IScope[]) => {
        if (tableLoaded) {
            const prevSelection = scopesTable.selection().map(row => row.__lparam.id);
            const newSelection = selection.map(row => row.id);
            const diffs = compare(prevSelection, newSelection);
            if (diffs.enter.length || diffs.exit.length) {
                scopesTable.selection(scopesTable.data().filter(row => {
                    return selection.indexOf(row[row.length - 1]) >= 0;
                }));
            }
        }
    }, [scopesTable, tableLoaded]);

    //  Graph  ---
    const metricGraph = useConst(() => new MetricGraph());
    const metricGraphWidget = useConst(() => new MetricGraphWidget()
        .zoomToFitLimit(1)
        .on("selectionChanged", () => {
            const selection = metricGraphWidget.selection().filter(id => metricGraph.item(id)).map(id => metricGraph.item(id).id);
            pushUrl(`${parentUrl}/${selection.join(",")}`);
        })
    );

    React.useEffect(() => {
        metricGraph.load(metrics);
    }, [metrics, metricGraph]);

    const updateMetricGraph = React.useCallback((selection: IScope[]) => {
        if (metricGraphWidget.renderCount() > 0) {
            //  Check if selection is already visible  ---
            const newSel = selection.map(s => s.name);
            if (!selection.length || selection.every(row => metricGraphWidget.exists(row.name))) {
                metricGraphWidget
                    .selection(newSel)
                    .render(() => {
                        if (trackSelection) {
                            metricGraphWidget.zoomToSelection();
                        }
                    })
                    ;
            } else {
                metricGraphWidget
                    .dot(metricGraph.graphTpl(selection, options))
                    .resize()
                    .render(() => {
                        metricGraphWidget.selection(newSel);
                    })
                    ;
            }
        }
    }, [metricGraph, metricGraphWidget, options, trackSelection]);

    const graphButtons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "selPrev", title: nlsHPCC.PreviousSelection, iconProps: { iconName: "NavigateBack" },
            disabled: selectedMetricsPtr < 1 || selectedMetricsPtr >= selectedMetrics.length,
            onClick: () => {
                metricGraphWidget.centerOnItem(selectedMetrics[selectedMetricsPtr - 1].name);
                setSelectedMetricsPtr(selectedMetricsPtr - 1);
            }
        },
        {
            key: "selNext", title: nlsHPCC.NextSelection, iconProps: { iconName: "NavigateBackMirrored" },
            disabled: selectedMetricsPtr < 0 || selectedMetricsPtr >= selectedMetrics.length - 1,
            onClick: () => {
                metricGraphWidget.centerOnItem(selectedMetrics[selectedMetricsPtr + 1].name);
                setSelectedMetricsPtr(selectedMetricsPtr + 1);
            }
        },
        {
            key: "reset", text: nlsHPCC.Reset, iconProps: { iconName: "Undo" },
            onClick: () => {
                metricGraphWidget.reset();
                setSelectedMetrics([]);
                setSelectedMetricsPtr(0);
                pushUrl(parentUrl);
            }
        }
    ], [metricGraphWidget, parentUrl, selectedMetrics, selectedMetricsPtr]);

    const graphRightButtons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "toSel", title: nlsHPCC.ZoomSelection,
            disabled: selectedMetrics.length <= 0,
            iconProps: { iconName: "FitPage" },
            canCheck: true,
            checked: trackSelection,
            onClick: () => {
                if (trackSelection) {
                    setTrackSelection(false);
                } else {
                    setTrackSelection(true);
                    metricGraphWidget.zoomToSelection();
                }
            }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
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
        },
    ], [metricGraphWidget, selectedMetrics.length, trackSelection]);

    const graphComponent = React.useMemo(() => {
        return <HolyGrail
            header={<CommandBar items={graphButtons} farItems={graphRightButtons} />}
            main={<AutosizeHpccJSComponent widget={metricGraphWidget} ></AutosizeHpccJSComponent>}
        />;
    }, [graphButtons, graphRightButtons, metricGraphWidget]);

    //  Props Table  ---
    const propsTable = useConst(() => new Table()
        .id("propsTable")
        .columns([nlsHPCC.Property, nlsHPCC.Value])
        .columnWidth("none")
    );

    const updatePropsTable = React.useCallback((selection: IScope[]) => {
        const props = [];
        selection.forEach((item, idx) => {
            for (const key in item) {
                if (key.indexOf("__") !== 0) {
                    props.push([key, item[key]]);
                }
            }
            if (idx < selection.length - 1) {
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

    const updatePropsTable2 = React.useCallback((selection: IScope[]) => {
        const columns = [];
        const props = [];
        selection.forEach(item => {
            for (const key in item) {
                if (key.indexOf("__") !== 0 && columns.indexOf(key) < 0) {
                    columns.push(key);
                }
            }
        });
        selection.forEach(item => {
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

    useDeepEffect(() => {
        if (selectedMetrics) {
            updateScopesTable(selectedMetrics);
            updateMetricGraph(selectedMetrics);
            updatePropsTable(selectedMetrics);
            updatePropsTable2(selectedMetrics);
        }
    }, [], [selectedMetrics]);

    React.useEffect(() => {
        const selectedIDs = selection.split(",");
        setSelectedMetrics(metrics.filter(m => selectedIDs.indexOf(m.id) >= 0));
        setSelectedMetricsPtr(0);
    }, [metrics, selection]);

    const items: DockPanelItems = React.useMemo<DockPanelItems>((): DockPanelItems => {
        return [
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
                component: graphComponent,
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
        ];
    }, [scopeFilter, onChangeScopeFilter, scopesTable, graphComponent, propsTable, propsTable2]);

    const layoutChanged = React.useCallback((layout) => {
        setOptions({ ...options, layout });
        saveOptions();
    }, [options, saveOptions, setOptions]);

    return <HolyGrail fullscreen={fullscreen}
        header={<>
            <CommandBar items={buttons} farItems={rightButtons} />
            <AutosizeHpccJSComponent widget={timeline} fixedHeight={"160px"} padding={4} hidden={!showTimeline} />
        </>}
        main={
            <ErrorBoundary>
                <DockPanel items={items} layout={options?.layout} layoutChanged={layoutChanged} onDockPanelCreate={setDockpanel} />
                <MetricsOptions show={showMetricOptions} setShow={setShowMetricOptions} />
            </ErrorBoundary>
        }
    />;
};
