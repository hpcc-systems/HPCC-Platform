import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, IIconProps, SearchBox } from "@fluentui/react";
import { Label, Spinner } from "@fluentui/react-components";
import { typographyStyles } from "@fluentui/react-theme";
import { useConst } from "@fluentui/react-hooks";
import { bundleIcon, Folder20Filled, Folder20Regular, FolderOpen20Filled, FolderOpen20Regular, } from "@fluentui/react-icons";
import { WorkunitsServiceEx, IScope } from "@hpcc-js/comms";
import { Table } from "@hpcc-js/dgrid";
import { compare, scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { WUTimelinePatched } from "src/Timings";
import * as Utility from "src/Utility";
import { FetchStatus, useMetricsOptions, useWorkunitMetrics } from "../hooks/metrics";
import { HolyGrail } from "../layouts/HolyGrail";
import { AutosizeComponent, AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { DockPanel, DockPanelItem, ResetableDockPanel } from "../layouts/DockPanel";
import { LayoutStatus, MetricGraph, MetricGraphWidget, isGraphvizWorkerResponse, layoutCache } from "../util/metricGraph";
import { pushUrl } from "../util/history";
import { debounce } from "../util/throttle";
import { ErrorBoundary } from "../util/errorBoundary";
import { ShortVerticalDivider } from "./Common";
import { MetricsOptions } from "./MetricsOptions";
import { BreadcrumbInfo, OverflowBreadcrumb } from "./controls/OverflowBreadcrumb";

const logger = scopedLogger("src-react/components/Metrics.tsx");

const filterIcon: IIconProps = { iconName: "Filter" };

const LineageIcon = bundleIcon(Folder20Filled, Folder20Regular);
const SelectedLineageIcon = bundleIcon(FolderOpen20Filled, FolderOpen20Regular);

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
    selection
}) => {
    const [_uiState, _setUIState] = React.useState({ ...defaultUIState });
    const [timelineFilter, setTimelineFilter] = React.useState("");
    const [selectedMetricsSource, setSelectedMetricsSource] = React.useState<"" | "scopesTable" | "metricGraphWidget" | "hotspot" | "reset">("");
    const [selectedMetrics, setSelectedMetrics] = React.useState<IScope[]>([]);
    const [selectedMetricsPtr, setSelectedMetricsPtr] = React.useState<number>(-1);
    const [metrics, columns, _activities, _properties, _measures, _scopeTypes, fetchStatus, refresh] = useWorkunitMetrics(wuid);
    const [showMetricOptions, setShowMetricOptions] = React.useState(false);
    const [options, setOptions, saveOptions] = useMetricsOptions();
    const [dockpanel, setDockpanel] = React.useState<ResetableDockPanel>();
    const [showTimeline, setShowTimeline] = React.useState<boolean>(true);
    const [trackSelection, setTrackSelection] = React.useState<boolean>(true);
    const [fullscreen, setFullscreen] = React.useState<boolean>(false);
    const [hotspots, setHotspots] = React.useState<string>("");
    const [lineage, setLineage] = React.useState<IScope[]>([]);
    const [selectedLineage, setSelectedLineage] = React.useState<IScope>();
    const [isLayoutComplete, setIsLayoutComplete] = React.useState<boolean>(false);
    const [isRenderComplete, setIsRenderComplete] = React.useState<boolean>(false);
    const [dot, setDot] = React.useState<string>("");

    React.useEffect(() => {
        const service = new WorkunitsServiceEx({ baseUrl: "" });
        service.WUAnalyseHotspot({
            Wuid: wuid,
            RootScope: "",
            OptOnlyActive: false,
            OnlyCriticalPath: false,
            IncludeProperties: true,
            IncludeStatistics: true,
            ThresholdPercent: 1.0,
            PropertyOptions: {
                IncludeName: true,
                IncludeRawValue: false,
                IncludeFormatted: true,
                IncludeMeasure: true,
                IncludeCreator: false,
                IncludeCreatorType: false
            }
        }).then(response => {
            setHotspots(response.Activities?.Activity?.map(activity => activity.Id).join(",") ?? "");
        }).catch(err => logger.error(err));
    }, [wuid]);

    const onHotspot = React.useCallback(() => {
        setSelectedMetricsSource("hotspot");
        pushUrl(`/workunits/${wuid}/metrics/${selection}`);
    }, [wuid, selection]);

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
            if (sel) {
                const selection = scopesTable.selection();
                setSelectedMetricsSource("scopesTable");
                pushUrl(`${parentUrl}/${selection.map(row => row.__lparam.id).join(",")}`);
            }
        }, 100))
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
            .render()
            ;
    }, [metrics, options.properties, options.scopeTypes, scopeFilterFunc, scopesTable, timelineFilter]);

    const updateScopesTable = React.useCallback((selection: IScope[]) => {
        if (scopesTable?.renderCount() > 0) {
            const prevSelection = scopesTable.selection().map(row => row.__lparam.id);
            const newSelection = selection.map(row => row.id);
            const diffs = compare(prevSelection, newSelection);
            if (diffs.enter.length || diffs.exit.length) {
                scopesTable.selection(scopesTable.data().filter(row => {
                    return selection.indexOf(row[row.length - 1]) >= 0;
                }));
            }
        }
    }, [scopesTable]);

    //  Graph  ---
    const metricGraph = useConst(() => new MetricGraph());
    const metricGraphWidget = useConst(() => new MetricGraphWidget()
        .zoomToFitLimit(1)
        .selectionGlowColor("DodgerBlue")
        .on("selectionChanged", () => {
            const selection = metricGraphWidget.selection().filter(id => metricGraph.item(id)).map(id => metricGraph.item(id).id);
            setSelectedMetricsSource("metricGraphWidget");
            pushUrl(`${parentUrl}/${selection.join(",")}`);
        })
    );

    React.useEffect(() => {
        metricGraph.load(metrics);
    }, [metrics, metricGraph]);

    const updateLineage = React.useCallback((selection: IScope[]) => {
        const newLineage: IScope[] = [];

        let minLen = Number.MAX_SAFE_INTEGER;
        const lineages = selection.map(item => {
            const retVal = metricGraph.lineage(item);
            minLen = Math.min(minLen, retVal.length);
            return retVal;
        });

        if (lineages.length) {
            for (let i = 0; i < minLen; ++i) {
                const item = lineages[0][i];
                if (lineages.every(lineage => lineage[i] === item)) {
                    if (item.id && item.type !== "child" && metricGraph.isSubgraph(item) && !metricGraph.isVertex(item)) {
                        newLineage.push(item);
                    }
                } else {
                    break;
                }
            }
        }

        setLineage(newLineage);
        if (!layoutCache.isComplete(dot) || newLineage.find(item => item === selectedLineage) === undefined) {
            setSelectedLineage(newLineage[newLineage.length - 1]);
        }
    }, [dot, metricGraph, selectedLineage]);

    const updateMetricGraph = React.useCallback((svg: string, selection: IScope[]) => {
        let cancelled = false;
        if (metricGraphWidget?.renderCount() > 0) {
            const sameSVG = metricGraphWidget.svg() === svg;
            setIsRenderComplete(sameSVG);
            metricGraphWidget
                .svg(svg)
                .visible(false)
                .resize()
                .render(() => {
                    if (!cancelled) {
                        const newSel = selection.map(s => s.name).filter(sel => !!sel);
                        metricGraphWidget
                            .visible(true)
                            .selection(newSel)
                            ;
                        if (trackSelection && selectedMetricsSource !== "metricGraphWidget") {
                            if (newSel.length) {
                                if (sameSVG) {
                                    metricGraphWidget.centerOnSelection();
                                } else {
                                    metricGraphWidget.zoomToSelection(0);
                                }
                            } else {
                                metricGraphWidget.zoomToFit(0);
                            }
                        }
                    }
                    setIsRenderComplete(true);
                })
                ;
        }
        return () => {
            cancelled = true;
        };
    }, [metricGraphWidget, selectedMetricsSource, trackSelection]);

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
        }
    ], [metricGraphWidget, selectedMetrics, selectedMetricsPtr]);

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

    const spinnerLabel: string = React.useMemo((): string => {
        if (fetchStatus === FetchStatus.STARTED) {
            return nlsHPCC.FetchingData;
        } else if (!isLayoutComplete) {
            return `${nlsHPCC.PerformingLayout} (${dot.split("\n").length})`;
        } else if (!isRenderComplete) {
            return nlsHPCC.RenderSVG;
        }
        return "";
    }, [fetchStatus, isLayoutComplete, isRenderComplete, dot]);

    const breadcrumbs = React.useMemo<BreadcrumbInfo[]>(() => {
        return lineage.map(item => {
            return {
                id: item.id,
                label: item.id,
                props: {
                    icon: selectedLineage === item ? <SelectedLineageIcon /> : <LineageIcon />
                }
            };
        });
    }, [lineage, selectedLineage]);

    //  Props Table  ---
    const propsTable = useConst(() => new Table()
        .columns([nlsHPCC.Property, nlsHPCC.Value, "Avg", "Min", "Max", "Delta", "StdDev", "SkewMin", "SkewMax", "NodeMin", "NodeMax"])
        .columnWidth("auto")
    );

    const updatePropsTable = React.useCallback((scopes: IScope[]) => {
        const props = [];
        scopes.forEach((item, idx) => {
            for (const key in item.__groupedProps) {
                const row = item.__groupedProps[key];
                props.push([row.Key, row.Value, row.Avg, row.Min, row.Max, row.Delta, row.StdDev, row.SkewMin, row.SkewMax, row.NodeMin, row.NodeMax]);
            }
            if (idx < scopes.length - 1) {
                props.push(["------------------------------", "------------------------------"]);
            }
        });
        propsTable
            ?.data(props)
            ?.lazyRender()
            ;
    }, [propsTable]);

    const propsTable2 = useConst(() => new Table()
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

    React.useEffect(() => {
        const dot = metricGraph.graphTpl(selectedLineage ? [selectedLineage] : [], options);
        setDot(dot);
    }, [metricGraph, options, selectedLineage]);

    React.useEffect(() => {
        let cancelled = false;
        if (metricGraphWidget?.renderCount() > 0) {
            setIsLayoutComplete(layoutCache.status(dot) === LayoutStatus.COMPLETED);
            layoutCache.calcSVG(dot).then(response => {
                if (!cancelled) {
                    if (isGraphvizWorkerResponse(response)) {
                        updateMetricGraph(response.svg, selectedMetrics?.length ? selectedMetrics : []);
                    }
                }
                setIsLayoutComplete(true);
            });
        }
        return () => {
            cancelled = true;
        };
    }, [dot, metricGraphWidget, selectedMetrics, updateMetricGraph]);

    React.useEffect(() => {
        if (selectedMetrics) {
            updateScopesTable(selectedMetrics);
            updatePropsTable(selectedMetrics);
            updatePropsTable2(selectedMetrics);
            updateLineage(selectedMetrics);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [selectedMetrics]);

    React.useEffect(() => {
        const selectedIDs = selection?.split(",") ?? [];
        setSelectedMetrics(metrics.filter(m => selectedIDs.indexOf(m.id) >= 0));
        setSelectedMetricsPtr(0);
    }, [metrics, selection]);

    React.useEffect(() => {

        //  Update layout prior to unmount  ---
        if (dockpanel && options && saveOptions && setOptions) {
            return () => {
                setOptions({ ...options, layout: dockpanel.getLayout() });
                saveOptions();
            };
        }
    }, [dockpanel, options, saveOptions, setOptions]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => {
                refresh();
            }
        },
        {
            key: "hotspot", text: nlsHPCC.Hotspots, iconProps: { iconName: "SpeedHigh" },
            disabled: !hotspots, onClick: () => onHotspot()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "timeline", text: nlsHPCC.Timeline, canCheck: true, checked: showTimeline, iconProps: { iconName: "TimelineProgress" },
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
    ], [dockpanel, hotspots, onHotspot, options, refresh, setOptions, showTimeline]);

    const formatColumns = React.useMemo((): Utility.ColumnMap => {
        const copyColumns: Utility.ColumnMap = {};
        for (const key in columns) {
            copyColumns[key] = {
                field: key,
                label: key
            };
        }
        return copyColumns;
    }, [columns]);

    const rightButtons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "copy", text: nlsHPCC.CopyToClipboard, disabled: !metrics.length || !navigator?.clipboard?.writeText, iconOnly: true, iconProps: { iconName: "Copy" },
            onClick: () => {
                const tsv = Utility.formatAsDelim(formatColumns, metrics, "\t");
                navigator?.clipboard?.writeText(tsv);
            }
        },
        {
            key: "download", text: nlsHPCC.DownloadToCSV, disabled: !metrics.length, iconOnly: true, iconProps: { iconName: "Download" },
            subMenuProps: {
                items: [{
                    key: "downloadCSV",
                    text: nlsHPCC.DownloadToCSV,
                    iconProps: { iconName: "Table" },
                    onClick: () => {
                        const csv = Utility.formatAsDelim(formatColumns, metrics, ",");
                        Utility.downloadCSV(csv, `metrics-${wuid}.csv`);
                    }
                },
                {
                    key: "downloadDOT",
                    text: nlsHPCC.DownloadToDOT,
                    iconProps: { iconName: "Relationship" },
                    onClick: () => {
                        Utility.downloadPlain(dot, `metrics-${wuid}.dot`);
                    }
                }]
            }
        }, {
            key: "fullscreen", title: nlsHPCC.MaximizeRestore, iconProps: { iconName: fullscreen ? "ChromeRestore" : "FullScreen" },
            onClick: () => setFullscreen(!fullscreen)
        }
    ], [dot, formatColumns, fullscreen, metrics, wuid]);

    return <HolyGrail fullscreen={fullscreen}
        header={<>
            <CommandBar items={buttons} farItems={rightButtons} />
            <AutosizeHpccJSComponent widget={timeline} fixedHeight={"160px"} padding={4} hidden={!showTimeline} />
        </>}
        main={
            <ErrorBoundary>
                <DockPanel layout={options?.layout} onDockPanelCreate={setDockpanel}>
                    <DockPanelItem key="scopesTable" title={nlsHPCC.Metrics}>
                        <HolyGrail
                            header={<SearchBox value={scopeFilter} onChange={onChangeScopeFilter} iconProps={filterIcon} placeholder={nlsHPCC.Filter} />}
                            main={<AutosizeHpccJSComponent widget={scopesTable} ></AutosizeHpccJSComponent>}
                        />
                    </DockPanelItem>
                    <DockPanelItem key="metricGraph" title={nlsHPCC.Graph} location="split-right" relativeTo="scopesTable" >
                        <HolyGrail
                            header={<>
                                <CommandBar items={graphButtons} farItems={graphRightButtons} />
                                <OverflowBreadcrumb breadcrumbs={breadcrumbs} selected={selectedLineage?.id} onSelect={item => setSelectedLineage(lineage.find(l => l.id === item.id))} />
                            </>}
                            main={<>
                                <AutosizeComponent hidden={!spinnerLabel}>
                                    <Spinner size="extra-large" label={spinnerLabel} labelPosition="below" ></Spinner>
                                </AutosizeComponent>
                                <AutosizeComponent hidden={!!spinnerLabel || selectedMetrics.length > 0}>
                                    <Label style={{ ...typographyStyles.subtitle2 }}>{nlsHPCC.NoContentPleaseSelectItem}</Label>
                                </AutosizeComponent>
                                <AutosizeHpccJSComponent widget={metricGraphWidget}>
                                </AutosizeHpccJSComponent>
                            </>
                            }
                        />
                    </DockPanelItem>
                    <DockPanelItem key="propsTable" title={nlsHPCC.Properties} location="split-bottom" relativeTo="scopesTable" >
                        <AutosizeHpccJSComponent widget={propsTable}></AutosizeHpccJSComponent>
                    </DockPanelItem>
                    <DockPanelItem key="propsTable2" title={nlsHPCC.CrossTab} location="tab-after" relativeTo="propsTable" >
                        <AutosizeHpccJSComponent widget={propsTable2}></AutosizeHpccJSComponent>
                    </DockPanelItem>
                </DockPanel>
                <MetricsOptions show={showMetricOptions} setShow={setShowMetricOptions} />
            </ErrorBoundary>
        }
    />;
};
