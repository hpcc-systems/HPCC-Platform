import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, TooltipHost } from "@fluentui/react";
import { makeStyles, SearchBox, SearchBoxChangeEvent, ToggleButton } from "@fluentui/react-components";
import { useConst } from "@fluentui/react-hooks";
import { TextCaseTitleRegular, TextCaseTitleFilled, BranchForkHintRegular, BranchForkFilled, TextWholeWordFilled, TextWholeWordRegular, FilterRegular } from "@fluentui/react-icons";
import { StackShim, StackItemShim } from "@fluentui/react-migration-v8-v9";
import { WorkunitsServiceEx, IScope } from "@hpcc-js/comms";
import { Table } from "@hpcc-js/dgrid";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { WUTimelineNoFetch } from "src/Timings";
import * as Utility from "src/Utility";
import { useMetricsViews, useWUQueryMetrics, scopeFilterMetrics, scopeFilterLogicalGraph } from "../hooks/metrics";
import { HolyGrail } from "../layouts/HolyGrail";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { DockPanel, DockPanelItem, ResetableDockPanel } from "../layouts/DockPanel";
import { pushUrl, replaceUrl } from "../util/history";
import { debounce } from "../util/throttle";
import { ErrorBoundary } from "../util/errorBoundary";
import { ShortVerticalDivider } from "./Common";
import { MetricsOptions } from "./MetricsOptions";
import { MetricsPropertiesTables } from "./MetricsPropertiesTables";
import { MetricsSQL } from "./MetricsSQL";
import { ScopesTable } from "./MetricsScopes";
import { useMetricsGraphData, MetricsGraph, calcLineage, idsToScopes } from "./MetricsGraph";
import { useUserTheme } from "../hooks/theme";

const logger = scopedLogger("src-react/components/Metrics.tsx");

const filterIcon = <FilterRegular />;

const useStyles = makeStyles({
    searchBox: {
        width: "100%",
        maxWidth: "none"
    }
});

type SelectedMetricsSource = "" | "scopesTable" | "scopesSqlTable" | "metricGraphWidget" | "hotspot" | "reset";
const TIMELINE_FIXEDHEIGHT = 152;

interface MetricsProps {
    wuid: string;
    logicalGraph?: boolean;
    targetsRoxie?: boolean;
    querySet?: string;
    queryId?: string;
    parentUrl?: string;
    viewSelection?: string;
    lineageSelection?: string;
    selection?: string[];
}

export const Metrics: React.FunctionComponent<MetricsProps> = ({
    wuid,
    logicalGraph = false,
    targetsRoxie = false,
    querySet = "",
    queryId = "",
    parentUrl = `/workunits/${wuid}/${logicalGraph ? "logicalgraph" : "metrics"}`,
    viewSelection,
    lineageSelection,
    selection
}) => {
    if (querySet && queryId) {
        wuid = "";
    }
    const styles = useStyles();
    const { isDark } = useUserTheme();
    const [selectedMetricsSource, setSelectedMetricsSource] = React.useState<SelectedMetricsSource>("");
    const { metrics, columns, status, refresh } = useWUQueryMetrics(wuid, querySet, queryId, logicalGraph ? scopeFilterLogicalGraph : scopeFilterMetrics);
    const { loaded, viewIds, viewId, setViewId, view, updateView } = useMetricsViews(logicalGraph);
    const metricGraphData = useMetricsGraphData(metrics, view, lineageSelection, selection);
    const { metricGraph, selectedMetrics, lineageSelectionScope, dot } = metricGraphData;
    const [showMetricOptions, setShowMetricOptions] = React.useState(false);
    const [dockpanel, setDockpanel] = React.useState<ResetableDockPanel>();
    const [hotspots, setHotspots] = React.useState<string>("");
    const [includePendingItems, setIncludePendingItems] = React.useState(true);
    const [matchCase, setMatchCase] = React.useState(false);
    const [matchWholeWord, setMatchWholeWord] = React.useState(false);

    const pushSelectionUrl = React.useCallback((parentUrl: string, viewId: string, lsName?: string, selection?: string[], replace: boolean = false) => {
        const hasLineage = !!lsName?.length;
        const hasSelection = !!selection?.length;
        const viewStr = (!logicalGraph && hasLineage && hasSelection) ? `/${encodeURIComponent(viewId)}` : "";
        const selectedMetrics = idsToScopes(metrics, selection);
        const lineage = calcLineage(metricGraph, selectedMetrics, lsName);
        const lineageSelectionStr = lineage.lineageSelectionScope?.name?.length ? `/${lineage.lineageSelectionScope.name}` : "";
        const selectionStr = selectedMetrics?.length ? `/${selectedMetrics.map(item => item.id).join(",")}` : "";
        if (replace) {
            replaceUrl(`${parentUrl}${viewStr}${lineageSelectionStr}${selectionStr}`);
        } else {
            pushUrl(`${parentUrl}${viewStr}${lineageSelectionStr}${selectionStr}`);
        }
    }, [logicalGraph, metricGraph, metrics]);

    //  Sync viewId FROM URL prop  ---
    const viewSynced = React.useRef(false);
    React.useEffect(() => {
        if (!loaded) return;
        if (!viewSynced.current) {
            viewSynced.current = true;
            const targetView = viewSelection || "Default";
            setViewId(viewIds.includes(targetView) ? targetView : "Default");
        }
    }, [loaded, viewSelection, viewIds, setViewId]);

    const selectionRef = React.useRef(selection);
    selectionRef.current = selection;

    const scopesTableClickRef = React.useRef(false);

    React.useEffect(() => {
        if (targetsRoxie && wuid) {
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
        }
    }, [targetsRoxie, wuid]);

    const pushSelectedMetricsUrl = React.useCallback((parentUrl: string, lsName: string, selectedMetrics: IScope[]) => {
        if (!lsName && selectedMetrics.length) {
            switch (selectedMetrics[0].type) {
                case "workflow":
                case "graph":
                case "subgraph":
                    lsName = selectedMetrics[0].name;
                    break;
                default:
                    lsName = selectedMetrics[0].__lparam.__parentName;
            }
        }
        let selection: string[];
        if (lsName && !selectedMetrics.length) {
            const parts = lsName.split(":");
            if (parts.length) {
                selection = [parts[parts.length - 1]];
            }
        } else {
            selection = selectedMetrics.map(row => row.__lparam?.id ?? row.id);
        }
        pushSelectionUrl(parentUrl, viewId, lsName, selection);
    }, [pushSelectionUrl, viewId]);

    const onHotspot = React.useCallback(() => {
        setSelectedMetricsSource("hotspot");
        pushSelectionUrl(parentUrl, viewId, lineageSelectionScope?.name, selection);
    }, [lineageSelectionScope?.name, parentUrl, pushSelectionUrl, selection, viewId]);

    //  Timeline ---
    const timeline = useConst(() => new WUTimelineNoFetch()
        .maxZoom(Number.MAX_SAFE_INTEGER)
    );

    const [scopeFilter, setScopeFilter] = React.useState("");
    const [scopeFilterVersion, setScopeFilterVersion] = React.useState(0);

    React.useEffect(() => {
        timeline
            .on("click", (row, col, sel) => {
                if (sel) {
                    timeline.selection([]);
                    setSelectedMetricsSource("scopesTable");
                    setScopeFilter(`name:${row[7].__hpcc_id}`);
                    setScopeFilterVersion(prev => prev + 1);
                    pushSelectedMetricsUrl(parentUrl, lineageSelectionScope?.name, [row[7]]);
                }
            }, true)
            ;
    }, [timeline, lineageSelectionScope?.name, parentUrl, pushSelectedMetricsUrl]);

    React.useEffect(() => {
        if (!logicalGraph && view.showTimeline) {
            timeline
                .scopes(metrics)
                .height(TIMELINE_FIXEDHEIGHT)
                .lazyRender()
                ;
        }
    }, [logicalGraph, metrics, timeline, view.showTimeline]);

    //  Scopes Table  ---
    const onChangeScopeFilter = React.useCallback((event: SearchBoxChangeEvent, data: { value: string }) => {
        setScopeFilter(data?.value ?? "");
    }, []);

    const scopesSelectionChanged = React.useCallback((source: SelectedMetricsSource, lsName?: string, selection: IScope[] = []) => {
        setSelectedMetricsSource(source);
        pushSelectedMetricsUrl(parentUrl, lsName, selection);
    }, [parentUrl, pushSelectedMetricsUrl]);

    const scopesTable = useConst(() => new ScopesTable()
        .multiSelect(true)
        .metrics([], view.scopeTypes, view.properties, scopeFilter, matchCase, matchWholeWord)
        .sortable(true)
    );

    React.useEffect(() => {
        scopesTable
            .on("click", debounce((row, col, sel) => {
                if (sel) {
                    scopesTableClickRef.current = true;
                    scopesSelectionChanged("scopesTable", lineageSelectionScope?.name, scopesTable.selection());
                }
            }), true)
            ;
    }, [scopesSelectionChanged, lineageSelectionScope?.name, scopesTable]);

    React.useEffect(() => {
        const scopesTableMetrics = includePendingItems ? metrics : metrics.filter(row => {
            return metricGraph.itemStatus(row) !== "unknown";
        });
        scopesTable
            .metrics(scopesTableMetrics, view.scopeTypes, view.properties, scopeFilter, matchCase, matchWholeWord)
            .lazyRender()
            ;
    }, [includePendingItems, matchCase, matchWholeWord, metricGraph, metrics, scopeFilter, scopesTable, view.properties, view.scopeTypes]);

    React.useEffect(() => {
        const currentSelection = selectionRef.current;
        if (!currentSelection?.length || !metrics?.length) return;
        const visibleIds = new Set<string>();
        scopesTable.data().forEach(row => {
            const scope: IScope = row[row.length - 1];
            visibleIds.add(scope.id);
        });
        const filteredSelection = currentSelection.filter(id => visibleIds.has(id));
        if (filteredSelection.length !== currentSelection.length) {
            pushSelectionUrl(parentUrl, viewId, filteredSelection.length ? lineageSelectionScope?.name : undefined, filteredSelection.length ? filteredSelection : undefined, true);
        }
    }, [includePendingItems, lineageSelectionScope?.name, matchCase, matchWholeWord, metricGraph, metrics, parentUrl, pushSelectionUrl, scopeFilter, scopesTable, view.properties, view.scopeTypes, viewId]);

    const updateScopesTable = React.useCallback((selection?: IScope[]) => {
        if (scopesTable?.renderCount() > 0) {
            if (scopesTableClickRef.current) {
                scopesTableClickRef.current = false;
                return;
            }
            scopesTable.selection([]);
            if (selection?.length) {
                const selRows = scopesTable.data().filter(row => {
                    return selection?.indexOf(row[row.length - 1]) >= 0;
                });
                scopesTable.render(() => {
                    scopesTable.selection(selRows);
                });
            }
        }
    }, [scopesTable]);

    React.useEffect(() => {
        scopesTable.columnFormats()[0]?.paletteID(isDark ? "StdDevsDark" : "StdDevs");
        scopesTable.render();
    }, [scopesTable, isDark]);

    //  Props Table  ---
    const crossTabTable = useConst(() => new Table()
        .columns([nlsHPCC.Property, nlsHPCC.Value])
        .columnWidth("auto")
        .sortable(true)
    );

    const updateCrossTabTable = React.useCallback((selection?: IScope[]) => {
        const columns = [];
        const props = [];
        selection?.forEach(item => {
            for (const key in item) {
                if (key.indexOf("__") !== 0 && columns.indexOf(key) < 0) {
                    columns.push(key);
                }
            }
        });
        selection?.forEach(item => {
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
        if (selectedMetrics) {
            updateScopesTable(selectedMetrics);
            updateCrossTabTable(selectedMetrics);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [selectedMetrics]);

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
            key: "hotspot", text: nlsHPCC.Hotspots, hidden: logicalGraph, iconProps: { iconName: "SpeedHigh" },
            disabled: !hotspots, onClick: () => onHotspot()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "views", text: viewId, hidden: logicalGraph, iconProps: { iconName: "View" },
            subMenuProps: {
                items: viewIds.map(v => ({
                    key: v, text: v, onClick: () => {
                        updateView({ layout: dockpanel.getLayout() });
                        setViewId(v);
                        pushSelectionUrl(parentUrl, v, lineageSelectionScope?.name, selection);
                    }
                }))
            },
        },
        {
            key: "timeline", text: nlsHPCC.Timeline, canCheck: true, checked: view.showTimeline, hidden: logicalGraph, iconProps: { iconName: "TimelineProgress" },
            onClick: () => {
                updateView({ showTimeline: !view.showTimeline }, true);
            }
        },
        {
            key: "options", text: nlsHPCC.Options, hidden: logicalGraph, iconProps: { iconName: "Settings" },
            onClick: () => {
                updateView({ layout: dockpanel.getLayout() });
                setShowMetricOptions(true);
            }
        }
    ].filter(item => item.hidden !== true), [dockpanel, hotspots, lineageSelectionScope?.name, logicalGraph, onHotspot, parentUrl, pushSelectionUrl, refresh, selection, setViewId, timeline, updateView, view.showTimeline, viewId, viewIds]);

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
        if (!show) {
            pushSelectionUrl(parentUrl, viewId, lineageSelectionScope?.name, selection);
        }
        setShowMetricOptions(show);
    }, [lineageSelectionScope?.name, parentUrl, pushSelectionUrl, selection, viewId]);

    const onLineageSelectionChange = React.useCallback((lsName?: string, replace?: boolean) => {
        pushSelectionUrl(parentUrl, viewId, lsName, selection, replace);
    }, [parentUrl, pushSelectionUrl, selection, viewId]);

    const onSelectionChange = React.useCallback((selection: string[]) => {
        setSelectedMetricsSource("metricGraphWidget");
        pushSelectionUrl(parentUrl, viewId, lineageSelectionScope?.name, selection);
    }, [lineageSelectionScope?.name, parentUrl, pushSelectionUrl, viewId]);

    return <HolyGrail
        header={<>
            <CommandBar items={buttons} farItems={rightButtons} />
            <AutosizeHpccJSComponent widget={timeline} fixedHeight={`${TIMELINE_FIXEDHEIGHT + 8}px`} padding={4} hidden={logicalGraph || !view.showTimeline} />
        </>}
        main={
            <ErrorBoundary>
                <DockPanel layout={view?.layout} onCreate={setDockpanel}>
                    <DockPanelItem key="scopesTable" title={nlsHPCC.Metrics}>
                        <HolyGrail
                            header={<StackShim horizontal tokens={{ childrenGap: 4 }}>
                                <ToggleButton appearance="subtle" icon={includePendingItems ? <BranchForkFilled /> : <BranchForkHintRegular />} title={nlsHPCC.IncludePendingItems} checked={includePendingItems} onClick={() => { setIncludePendingItems(!includePendingItems); }} />
                                <StackItemShim grow>
                                    <TooltipHost content={nlsHPCC.FilterMetricsTooltip}>
                                        <SearchBox key={scopeFilterVersion} defaultValue={scopeFilter} onChange={onChangeScopeFilter} placeholder={nlsHPCC.Filter} contentBefore={filterIcon} className={styles.searchBox} />
                                    </TooltipHost>
                                </StackItemShim>
                                <ToggleButton appearance="subtle" icon={matchCase ? <TextCaseTitleFilled /> : <TextCaseTitleRegular />} title={nlsHPCC.MatchCase} checked={matchCase} onClick={() => { setMatchCase(!matchCase); }} />
                                <ToggleButton appearance="subtle" icon={matchWholeWord ? <TextWholeWordFilled /> : <TextWholeWordRegular />} title={nlsHPCC.MatchWholeWord} checked={matchWholeWord} onClick={() => { setMatchWholeWord(!matchWholeWord); }} />
                            </StackShim>}
                            main={<AutosizeHpccJSComponent widget={scopesTable} ></AutosizeHpccJSComponent>}
                        />
                    </DockPanelItem>
                    <DockPanelItem key="metricsSql" title={nlsHPCC.MetricsSQL} location="tab-after" relativeTo="scopesTable">
                        <MetricsSQL wuid={wuid} defaultSql={view.sql} scopes={metrics} onSelectionChanged={selection => scopesSelectionChanged("scopesSqlTable", lineageSelectionScope?.name, selection)}></MetricsSQL>
                    </DockPanelItem>
                    <DockPanelItem key="metricGraph" title={nlsHPCC.Graph} location="split-right" relativeTo="scopesTable" >
                        <MetricsGraph
                            metrics={metrics}
                            metricGraphData={metricGraphData}
                            selection={selection}
                            selectedMetricsSource={selectedMetricsSource}
                            status={status}
                            onLineageSelectionChange={onLineageSelectionChange}
                            onSelectionChange={onSelectionChange}>
                        </MetricsGraph>
                    </DockPanelItem>
                    <DockPanelItem key="propsTable" title={nlsHPCC.Properties} location="split-bottom" relativeTo="scopesTable" >
                        <MetricsPropertiesTables wuid={wuid} querySet={querySet} queryId={queryId} scopesTableColumns={scopesTable.columns()} scopes={selectedMetrics}></MetricsPropertiesTables>
                    </DockPanelItem>
                    <DockPanelItem key="propsTable2" title={nlsHPCC.CrossTab} location="tab-after" relativeTo="propsTable" >
                        <AutosizeHpccJSComponent widget={crossTabTable}></AutosizeHpccJSComponent>
                    </DockPanelItem>
                </DockPanel>
                <MetricsOptions show={showMetricOptions} setShow={setShowMetricOptionsHook} logicalGraph={logicalGraph} />
            </ErrorBoundary >
        }
    />;
};
