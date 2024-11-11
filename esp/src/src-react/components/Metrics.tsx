import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, IIconProps, SearchBox, Stack } from "@fluentui/react";
import { Label, Spinner, ToggleButton } from "@fluentui/react-components";
import { typographyStyles } from "@fluentui/react-theme";
import { useConst } from "@fluentui/react-hooks";
import { bundleIcon, Folder20Filled, Folder20Regular, FolderOpen20Filled, FolderOpen20Regular, TextCaseTitleRegular, TextCaseTitleFilled, BranchForkHintRegular, BranchForkFilled } from "@fluentui/react-icons";
import { WorkunitsServiceEx, IScope } from "@hpcc-js/comms";
import { Table } from "@hpcc-js/dgrid";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { WUTimelineNoFetch } from "src/Timings";
import * as Utility from "src/Utility";
import { FetchStatus, useMetricsViews, useWUQueryMetrics } from "../hooks/metrics";
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
import { MetricsPropertiesTables } from "./MetricsPropertiesTables";
import { MetricsSQL } from "./MetricsSQL";
import { ScopesTable } from "./MetricsScopes";

const logger = scopedLogger("src-react/components/Metrics.tsx");

const filterIcon: IIconProps = { iconName: "Filter" };

const LineageIcon = bundleIcon(Folder20Filled, Folder20Regular);
const SelectedLineageIcon = bundleIcon(FolderOpen20Filled, FolderOpen20Regular);

const defaultUIState = {
    hasSelection: false
};

type SelectedMetricsSource = "" | "scopesTable" | "scopesSqlTable" | "metricGraphWidget" | "hotspot" | "reset";
const TIMELINE_FIXEDHEIGHT = 152;

interface MetricsProps {
    wuid: string;
    querySet?: string;
    queryId?: string;
    parentUrl?: string;
    selection?: string;
}

export const Metrics: React.FunctionComponent<MetricsProps> = ({
    wuid,
    querySet = "",
    queryId = "",
    parentUrl = `/workunits/${wuid}/metrics`,
    selection
}) => {
    if (querySet && queryId) {
        wuid = "";
    }
    const [_uiState, _setUIState] = React.useState({ ...defaultUIState });
    const [selectedMetricsSource, setSelectedMetricsSource] = React.useState<SelectedMetricsSource>("");
    const [selectedMetrics, setSelectedMetrics] = React.useState<IScope[]>([]);
    const [selectedMetricsPtr, setSelectedMetricsPtr] = React.useState<number>(-1);
    const { metrics, columns, status, refresh } = useWUQueryMetrics(wuid, querySet, queryId);
    const { viewIds, viewId, setViewId, view, updateView } = useMetricsViews();
    const [showMetricOptions, setShowMetricOptions] = React.useState(false);
    const [dockpanel, setDockpanel] = React.useState<ResetableDockPanel>();
    const [trackSelection, setTrackSelection] = React.useState<boolean>(true);
    const [hotspots, setHotspots] = React.useState<string>("");
    const [lineage, setLineage] = React.useState<IScope[]>([]);
    const [selectedLineage, setSelectedLineage] = React.useState<IScope>();
    const [isLayoutComplete, setIsLayoutComplete] = React.useState<boolean>(false);
    const [isRenderComplete, setIsRenderComplete] = React.useState<boolean>(false);
    const [dot, setDot] = React.useState<string>("");
    const [includePendingItems, setIncludePendingItems] = React.useState(false);
    const [matchCase, setMatchCase] = React.useState(false);

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

    const pushSelectionUrl = React.useCallback((selection: string) => {
        const selectionStr = selection?.length ? `/${selection}` : "";
        pushUrl(`${parentUrl}${selectionStr}`);
    }, [parentUrl]);

    const onHotspot = React.useCallback(() => {
        setSelectedMetricsSource("hotspot");
        pushSelectionUrl(selection);
    }, [pushSelectionUrl, selection]);

    //  Timeline ---
    const timeline = useConst(() => new WUTimelineNoFetch()
        .maxZoom(Number.MAX_SAFE_INTEGER)
    );

    const [scopeFilter, setScopeFilter] = React.useState("");
    React.useEffect(() => {
        timeline
            .on("click", (row, col, sel) => {
                if (sel) {
                    timeline.selection([]);
                    setSelectedMetricsSource("scopesTable");
                    setScopeFilter(`name:${row[7].__hpcc_id}`);
                    pushSelectionUrl(row[7].id);
                }
            }, true)
            ;
    }, [pushSelectionUrl, timeline]);

    React.useEffect(() => {
        if (view.showTimeline) {
            timeline
                .scopes(metrics)
                .height(TIMELINE_FIXEDHEIGHT)
                .lazyRender()
                ;
        }
    }, [metrics, timeline, view.showTimeline]);

    //  Graph  ---
    const metricGraph = useConst(() => new MetricGraph());
    const metricGraphWidget = useConst(() => new MetricGraphWidget()
        .zoomToFitLimit(1)
        .selectionGlowColor("DodgerBlue")
    );

    React.useEffect(() => {
        metricGraphWidget
            .on("selectionChanged", () => {
                const selection = metricGraphWidget.selection().filter(id => metricGraph.item(id)).map(id => metricGraph.item(id).id);
                setSelectedMetricsSource("metricGraphWidget");
                pushSelectionUrl(selection.join(","));
            }, true)
            ;
    }, [metricGraph, metricGraphWidget, pushSelectionUrl]);

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
        if (status === FetchStatus.STARTED) {
            return nlsHPCC.FetchingData;
        } else if (!isLayoutComplete) {
            return `${nlsHPCC.PerformingLayout}(${dot.split("\n").length})`;
        } else if (!isRenderComplete) {
            return nlsHPCC.RenderSVG;
        }
        return "";
    }, [status, isLayoutComplete, isRenderComplete, dot]);

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

    //  Scopes Table  ---
    const onChangeScopeFilter = React.useCallback((event: React.FormEvent<HTMLInputElement | HTMLTextAreaElement>, newValue?: string) => {
        setScopeFilter(newValue || "");
    }, []);

    const scopesSelectionChanged = React.useCallback((source: SelectedMetricsSource, selection: IScope[]) => {
        setSelectedMetricsSource(source);
        pushSelectionUrl(selection.map(row => row.__lparam?.id ?? row.id).join(","));
    }, [pushSelectionUrl]);

    const scopesTable = useConst(() => new ScopesTable()
        .multiSelect(true)
        .metrics([], view.scopeTypes, view.properties, scopeFilter, matchCase)
        .sortable(true)
    );

    React.useEffect(() => {
        scopesTable
            .on("click", debounce((row, col, sel) => {
                if (sel) {
                    scopesSelectionChanged("scopesTable", scopesTable.selection());
                }
            }), true)
            ;
    }, [scopesSelectionChanged, scopesTable]);

    React.useEffect(() => {
        const scopesTableMetrics = includePendingItems ? metrics : metrics.filter(row => {
            if (metricGraph.isVertex(row)) {
                return metricGraph.vertexStatus(row) !== "unknown";
            } else if (metricGraph.isEdge(row)) {
                return metricGraph.edgeStatus(row) !== "unknown";
            } else if (metricGraph.isSubgraph(row)) {
                return metricGraph.subgraphStatus(row) !== "unknown";
            }
            return true;
        });
        scopesTable
            .metrics(scopesTableMetrics, view.scopeTypes, view.properties, scopeFilter, matchCase)
            .lazyRender()
            ;
    }, [includePendingItems, matchCase, metricGraph, metrics, scopeFilter, scopesTable, view.properties, view.scopeTypes]);

    const updateScopesTable = React.useCallback((selection: IScope[]) => {
        if (scopesTable?.renderCount() > 0 && selectedMetricsSource !== "scopesTable") {
            scopesTable.selection([]);
            if (selection.length) {
                const selRows = scopesTable.data().filter(row => {
                    return selection.indexOf(row[row.length - 1]) >= 0;
                });
                scopesTable.render(() => {
                    scopesTable.selection(selRows);
                });
            }
        }
    }, [scopesTable, selectedMetricsSource]);

    //  Props Table  ---
    const crossTabTable = useConst(() => new Table()
        .columns([nlsHPCC.Property, nlsHPCC.Value])
        .columnWidth("auto")
        .sortable(true)
    );

    const updateCrossTabTable = React.useCallback((selection: IScope[]) => {
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
        crossTabTable
            .columns([])
            .columns(columns)
            .data(props)
            .lazyRender()
            ;
    }, [crossTabTable]);

    React.useEffect(() => {
        const dot = metricGraph.graphTpl(selectedLineage ? [selectedLineage] : [], view);
        setDot(dot);
    }, [metricGraph, view, selectedLineage]);

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
            }).catch(err => logger.error(err));
        }
        return () => {
            cancelled = true;
        };
    }, [dot, metricGraphWidget, selectedMetrics, updateMetricGraph]);

    React.useEffect(() => {
        if (selectedMetrics) {
            updateScopesTable(selectedMetrics);
            updateCrossTabTable(selectedMetrics);
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
        if (dockpanel && updateView) {
            return () => {
                if (dockpanel && updateView) {
                    updateView({ layout: dockpanel.getLayout() });
                }
            };
        }
    }, [dockpanel, updateView]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => {
                refresh();
                timeline
                    .clear()
                    .lazyRender()
                    ;
            }
        },
        {
            key: "hotspot", text: nlsHPCC.Hotspots, iconProps: { iconName: "SpeedHigh" },
            disabled: !hotspots, onClick: () => onHotspot()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "views", text: viewId, iconProps: { iconName: "View" },
            subMenuProps: {
                items: viewIds.map(v => ({
                    key: v, text: v, onClick: () => {
                        updateView({ layout: dockpanel.getLayout() });
                        setViewId(v);
                    }
                }))
            },
        },
        {
            key: "timeline", text: nlsHPCC.Timeline, canCheck: true, checked: view.showTimeline, iconProps: { iconName: "TimelineProgress" },
            onClick: () => {
                updateView({ showTimeline: !view.showTimeline }, true);
            }
        },
        {
            key: "options", text: nlsHPCC.Options, iconProps: { iconName: "Settings" },
            onClick: () => {
                updateView({ layout: dockpanel.getLayout() });
                setShowMetricOptions(true);
            }
        }
    ], [dockpanel, hotspots, onHotspot, refresh, setViewId, timeline, updateView, view.showTimeline, viewId, viewIds]);

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
        },
    ], [dot, formatColumns, metrics, wuid]);

    const setShowMetricOptionsHook = React.useCallback((show: boolean) => {
        setShowMetricOptions(show);
    }, []);

    return <HolyGrail
        header={<>
            <CommandBar items={buttons} farItems={rightButtons} />
            <AutosizeHpccJSComponent widget={timeline} fixedHeight={`${TIMELINE_FIXEDHEIGHT + 8}px`} padding={4} hidden={!view.showTimeline} />
        </>}
        main={
            <ErrorBoundary>
                <DockPanel layout={view?.layout} onCreate={setDockpanel}>
                    <DockPanelItem key="scopesTable" title={nlsHPCC.Metrics}>
                        <HolyGrail
                            header={<Stack horizontal>
                                <ToggleButton appearance="subtle" icon={matchCase ? <BranchForkFilled /> : <BranchForkHintRegular />} title={nlsHPCC.IncludePendingItems} checked={includePendingItems} onClick={() => { setIncludePendingItems(!includePendingItems); }} />
                                <Stack.Item grow>
                                    <SearchBox value={scopeFilter} onChange={onChangeScopeFilter} iconProps={filterIcon} placeholder={nlsHPCC.Filter} />
                                </Stack.Item>
                                <ToggleButton appearance="subtle" icon={matchCase ? <TextCaseTitleFilled /> : <TextCaseTitleRegular />} title={nlsHPCC.MatchCase} checked={matchCase} onClick={() => { setMatchCase(!matchCase); }} />
                            </Stack>}
                            main={<AutosizeHpccJSComponent widget={scopesTable} ></AutosizeHpccJSComponent>}
                        />
                    </DockPanelItem>
                    <DockPanelItem key="metricsSql" title={nlsHPCC.MetricsSQL} location="tab-after" relativeTo="scopesTable">
                        <MetricsSQL defaultSql={view.sql} scopes={metrics} onSelectionChanged={selection => scopesSelectionChanged("scopesSqlTable", selection)}></MetricsSQL>
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
                        <MetricsPropertiesTables scopesTableColumns={scopesTable.columns()} scopes={selectedMetrics}></MetricsPropertiesTables>
                    </DockPanelItem>
                    <DockPanelItem key="propsTable2" title={nlsHPCC.CrossTab} location="tab-after" relativeTo="propsTable" >
                        <AutosizeHpccJSComponent widget={crossTabTable}></AutosizeHpccJSComponent>
                    </DockPanelItem>
                </DockPanel>
                <MetricsOptions show={showMetricOptions} setShow={setShowMetricOptionsHook} />
            </ErrorBoundary>
        }
    />;
};
