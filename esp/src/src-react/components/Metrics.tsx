import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, IIconProps, SearchBox, Stack } from "@fluentui/react";
import { Label, Spinner, ToggleButton } from "@fluentui/react-components";
import { typographyStyles } from "@fluentui/react-theme";
import { useConst } from "@fluentui/react-hooks";
import { bundleIcon, Folder20Filled, Folder20Regular, FolderOpen20Filled, FolderOpen20Regular, TextCaseTitleRegular, TextCaseTitleFilled } from "@fluentui/react-icons";
import { Database } from "@hpcc-js/common";
import { WorkunitsServiceEx, IScope, splitMetric } from "@hpcc-js/comms";
import { CellFormatter, ColumnFormat, ColumnType, DBStore, RowType, Table } from "@hpcc-js/dgrid";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { WUTimelineNoFetch } from "src/Timings";
import * as Utility from "src/Utility";
import { FetchStatus, useMetricsOptions, useWUQueryMetrics, MetricsOptions as MetricsOptionsT } from "../hooks/metrics";
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

const logger = scopedLogger("src-react/components/Metrics.tsx");

const filterIcon: IIconProps = { iconName: "Filter" };

const LineageIcon = bundleIcon(Folder20Filled, Folder20Regular);
const SelectedLineageIcon = bundleIcon(FolderOpen20Filled, FolderOpen20Regular);

const defaultUIState = {
    hasSelection: false
};

class ColumnFormatEx extends ColumnFormat {
    formatterFunc(): CellFormatter | undefined {
        const colIdx = this._owner.columns().indexOf("__StdDevs");

        return function (this: ColumnType, cell: any, row: RowType): string {
            return row[colIdx];
        };
    }
}

class DBStoreEx extends DBStore {

    constructor(protected _table: TableEx, db: Database.Grid) {
        super(db);
    }

    sort(opts) {
        this._table.sort(opts);
        return this;
    }
}

class TableEx extends Table {

    constructor() {
        super();
        this._store = new DBStoreEx(this, this._db);
    }

    scopeFilterFunc(row: object, scopeFilter: string, matchCase: boolean): boolean {
        const filter = scopeFilter.trim();
        if (filter) {
            let field = "";
            const colonIdx = filter.indexOf(":");
            if (colonIdx > 0) {
                field = filter.substring(0, colonIdx);
            }
            if (field) {
                const value: string = !matchCase ? row[field]?.toString().toLowerCase() : row[field]?.toString();
                const filterValue: string = !matchCase ? filter.toLowerCase() : filter;
                return value?.indexOf(filterValue.substring(colonIdx + 1)) >= 0 ?? false;
            }
            for (const field in row) {
                const value: string = !matchCase ? row[field].toString().toLowerCase() : row[field].toString();
                const filterValue: string = !matchCase ? filter.toLowerCase() : filter;
                return value?.indexOf(filterValue) >= 0 ?? false;
            }
            return false;
        }
        return true;
    }

    _rawDataMap: { [id: number]: string } = {};
    metrics(metrics: any[], options: MetricsOptionsT, scopeFilter: string, matchCase: boolean): this {
        this
            .columns(["##"])    //  Reset hash to force recalculation of default widths
            .columns(["##", nlsHPCC.Type, "StdDevs", nlsHPCC.Scope, ...options.properties, "__StdDevs"])
            .columnFormats([
                new ColumnFormatEx()
                    .column("StdDevs")
                    .paletteID("StdDevs")
                    .min(0)
                    .max(6),
                new ColumnFormat()
                    .column("__StdDevs")
                    .width(0)
            ])
            .data(metrics
                .filter(m => this.scopeFilterFunc(m, scopeFilter, matchCase))
                .filter(row => {
                    return options.scopeTypes.indexOf(row.type) >= 0;
                }).map((row, idx) => {
                    if (idx === 0) {
                        this._rawDataMap = {
                            0: "##", 1: "type", 2: "__StdDevs", 3: "name"
                        };
                        options.properties.forEach((p, idx2) => {
                            this._rawDataMap[4 + idx2] = p;
                        });
                    }
                    row.__hpcc_id = row.name;
                    return [idx, row.type, row.__StdDevs === 0 ? undefined : row.__StdDevs, row.name, ...options.properties.map(p => {
                        return row.__groupedProps[p]?.Value ??
                            row.__groupedProps[p]?.Max ??
                            row.__groupedProps[p]?.Avg ??
                            row.__formattedProps[p] ??
                            row[p] ??
                            "";
                    }), row.__StdDevsSource, row];
                }))
            ;
        return this;
    }

    sort(opts) {
        const optsEx = opts.map(opt => {
            return {
                idx: opt.property,
                metricLabel: this._rawDataMap[opt.property],
                splitMetricLabel: splitMetric(this._rawDataMap[opt.property]),
                descending: opt.descending
            };
        });

        const lparamIdx = this.columns().length;
        this._db.data().sort((l, r) => {
            const llparam = l[lparamIdx];
            const rlparam = r[lparamIdx];
            for (const { idx, metricLabel, splitMetricLabel, descending } of optsEx) {
                const lval = llparam[metricLabel] ?? llparam[`${splitMetricLabel.measure}Max${splitMetricLabel.label}`] ?? llparam[`${splitMetricLabel.measure}Avg${splitMetricLabel.label}`] ?? l[idx];
                const rval = rlparam[metricLabel] ?? rlparam[`${splitMetricLabel.measure}Max${splitMetricLabel.label}`] ?? rlparam[`${splitMetricLabel.measure}Avg${splitMetricLabel.label}`] ?? r[idx];
                if ((lval === undefined && rval !== undefined) || lval < rval) return descending ? 1 : -1;
                if ((lval !== undefined && rval === undefined) || lval > rval) return descending ? -1 : 1;
            }
            return 0;
        });
        return this;
    }
}

type SelectedMetricsSource = "" | "scopesTable" | "scopesSqlTable" | "metricGraphWidget" | "hotspot" | "reset";

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
    const [_uiState, _setUIState] = React.useState({ ...defaultUIState });
    const [selectedMetricsSource, setSelectedMetricsSource] = React.useState<SelectedMetricsSource>("");
    const [selectedMetrics, setSelectedMetrics] = React.useState<IScope[]>([]);
    const [selectedMetricsPtr, setSelectedMetricsPtr] = React.useState<number>(-1);
    const [metrics, columns, _activities, _properties, _measures, _scopeTypes, fetchStatus, refresh] = useWUQueryMetrics(wuid, querySet, queryId);
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

    const onHotspot = React.useCallback(() => {
        setSelectedMetricsSource("hotspot");
        pushUrl(`${parentUrl}/${selection}`);
    }, [parentUrl, selection]);

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
                    pushUrl(`${parentUrl}/${row[7].id}`);
                }
            }, true)
            ;
    }, [parentUrl, timeline]);

    React.useEffect(() => {
        timeline
            .scopes(metrics)
            .lazyRender()
            ;
    }, [metrics, timeline]);

    //  Scopes Table  ---
    const onChangeScopeFilter = React.useCallback((event: React.FormEvent<HTMLInputElement | HTMLTextAreaElement>, newValue?: string) => {
        setScopeFilter(newValue || "");
    }, []);

    const scopesSelectionChanged = React.useCallback((source: SelectedMetricsSource, selection: IScope[]) => {
        setSelectedMetricsSource(source);
        pushUrl(`${parentUrl}/${selection.map(row => row.__lparam?.id ?? row.id).join(",")}`);
    }, [parentUrl]);

    const scopesTable = useConst(() => new TableEx()
        .multiSelect(true)
        .metrics([], options, scopeFilter, matchCase)
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
        scopesTable
            .metrics(metrics, options, scopeFilter, matchCase)
            .lazyRender()
            ;
    }, [matchCase, metrics, options, scopeFilter, scopesTable]);

    const updateScopesTable = React.useCallback((selection: IScope[]) => {
        if (scopesTable?.renderCount() > 0) {
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
    }, [scopesTable]);

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
                pushUrl(`${parentUrl}/${selection.join(",")}`);
            }, true)
            ;
    }, [metricGraph, metricGraphWidget, parentUrl]);

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
            return `${nlsHPCC.PerformingLayout}(${dot.split("\n").length})`;
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
    ], [dockpanel, hotspots, onHotspot, options, refresh, setOptions, showTimeline, timeline]);

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

    const setShowMetricOptionsHook = React.useCallback((show: boolean) => {
        setShowMetricOptions(show);
        scopesTable
            .metrics(metrics, options, scopeFilter, matchCase)
            .render(() => {
                updateScopesTable(selectedMetrics);
            })
            ;

    }, [matchCase, metrics, options, scopeFilter, scopesTable, selectedMetrics, updateScopesTable]);

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
                            header={<Stack horizontal>
                                <Stack.Item grow>
                                    <SearchBox value={scopeFilter} onChange={onChangeScopeFilter} iconProps={filterIcon} placeholder={nlsHPCC.Filter} />
                                </Stack.Item>
                                <ToggleButton appearance="subtle" icon={matchCase ? <TextCaseTitleFilled /> : <TextCaseTitleRegular />} title={nlsHPCC.MatchCase} checked={matchCase} onClick={() => { setMatchCase(!matchCase); }} />
                            </Stack>}
                            main={<AutosizeHpccJSComponent widget={scopesTable} ></AutosizeHpccJSComponent>}
                        />
                    </DockPanelItem>
                    <DockPanelItem key="metricsSql" title={nlsHPCC.MetricsSQL} location="tab-after" relativeTo="scopesTable">
                        <MetricsSQL defaultSql={options.sql} scopes={metrics} onSelectionChanged={selection => scopesSelectionChanged("scopesSqlTable", selection)}></MetricsSQL>
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
