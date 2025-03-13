import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, IIconProps, SearchBox, Stack, TooltipHost } from "@fluentui/react";
import { ToggleButton } from "@fluentui/react-components";
import { useConst } from "@fluentui/react-hooks";
import { TextCaseTitleRegular, TextCaseTitleFilled, BranchForkHintRegular, BranchForkFilled } from "@fluentui/react-icons";
import { WorkunitsServiceEx, IScope } from "@hpcc-js/comms";
import { Table } from "@hpcc-js/dgrid";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { WUTimelineNoFetch } from "src/Timings";
import * as Utility from "src/Utility";
import { useMetricsViews, useWUQueryMetrics } from "../hooks/metrics";
import { HolyGrail } from "../layouts/HolyGrail";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { DockPanel, DockPanelItem, ResetableDockPanel } from "../layouts/DockPanel";
import { pushUrl } from "../util/history";
import { debounce } from "../util/throttle";
import { ErrorBoundary } from "../util/errorBoundary";
import { ShortVerticalDivider } from "./Common";
import { MetricsOptions } from "./MetricsOptions";
import { MetricsPropertiesTables } from "./MetricsPropertiesTables";
import { MetricsSQL } from "./MetricsSQL";
import { ScopesTable } from "./MetricsScopes";
import { useMetricsGraphData, MetricsGraph } from "./MetricsGraph";

const logger = scopedLogger("src-react/components/Metrics.tsx");

const filterIcon: IIconProps = { iconName: "Filter" };

type SelectedMetricsSource = "" | "scopesTable" | "scopesSqlTable" | "metricGraphWidget" | "hotspot" | "reset";
const TIMELINE_FIXEDHEIGHT = 152;

const pushSelectionUrl = (parentUrl: string, lineageSelection?: string, selection?: string[]) => {
    const lineageSelectionStr = lineageSelection?.length ? `/${lineageSelection}` : "";
    const selectionStr = selection?.length ? `/${selection.join(",")}` : "";
    pushUrl(`${parentUrl}${lineageSelectionStr}${selectionStr}`);
};

interface MetricsProps {
    wuid: string;
    querySet?: string;
    queryId?: string;
    parentUrl?: string;
    lineageSelection?: string;
    selection?: string[];
}

export const Metrics: React.FunctionComponent<MetricsProps> = ({
    wuid,
    querySet = "",
    queryId = "",
    parentUrl = `/workunits/${wuid}/metrics`,
    lineageSelection,
    selection
}) => {
    if (querySet && queryId) {
        wuid = "";
    }
    const [selectedMetricsSource, setSelectedMetricsSource] = React.useState<SelectedMetricsSource>("");
    const { metrics, columns, status, refresh } = useWUQueryMetrics(wuid, querySet, queryId);
    const { viewIds, viewId, setViewId, view, updateView } = useMetricsViews();
    const metricGraphData = useMetricsGraphData(metrics, view, lineageSelection, selection);
    const { metricGraph, selectedMetrics, dot } = metricGraphData;
    const [showMetricOptions, setShowMetricOptions] = React.useState(false);
    const [dockpanel, setDockpanel] = React.useState<ResetableDockPanel>();
    const [hotspots, setHotspots] = React.useState<string>("");
    const [includePendingItems, setIncludePendingItems] = React.useState(false);
    const [matchCase, setMatchCase] = React.useState(false);

    React.useEffect(() => {
        if (wuid) {
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
    }, [wuid]);

    const onHotspot = React.useCallback(() => {
        setSelectedMetricsSource("hotspot");
        pushSelectionUrl(parentUrl, lineageSelection, selection);
    }, [lineageSelection, parentUrl, selection]);

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
                    pushSelectionUrl(parentUrl, lineageSelection, [row[7].id]);
                }
            }, true)
            ;
    }, [timeline, lineageSelection, parentUrl]);

    React.useEffect(() => {
        if (view.showTimeline) {
            timeline
                .scopes(metrics)
                .height(TIMELINE_FIXEDHEIGHT)
                .lazyRender()
                ;
        }
    }, [metrics, timeline, view.showTimeline]);

    //  Scopes Table  ---
    const onChangeScopeFilter = React.useCallback((event: React.FormEvent<HTMLInputElement | HTMLTextAreaElement>, newValue?: string) => {
        setScopeFilter(newValue || "");
    }, []);

    const scopesSelectionChanged = React.useCallback((source: SelectedMetricsSource, lineageSelection?: string, selection: IScope[] = []) => {
        setSelectedMetricsSource(source);
        pushSelectionUrl(parentUrl, lineageSelection, selection.map(row => row.__lparam?.id ?? row.id));
    }, [parentUrl]);

    const scopesTable = useConst(() => new ScopesTable()
        .multiSelect(true)
        .metrics([], view.scopeTypes, view.properties, scopeFilter, matchCase)
        .sortable(true)
    );

    React.useEffect(() => {
        scopesTable
            .on("click", debounce((row, col, sel) => {
                if (sel) {
                    scopesSelectionChanged("scopesTable", lineageSelection, scopesTable.selection());
                }
            }), true)
            ;
    }, [scopesSelectionChanged, lineageSelection, scopesTable]);

    React.useEffect(() => {
        const scopesTableMetrics = includePendingItems ? metrics : metrics.filter(row => {
            return metricGraph.itemStatus(row) !== "unknown";
        });
        scopesTable
            .metrics(scopesTableMetrics, view.scopeTypes, view.properties, scopeFilter, matchCase)
            .lazyRender()
            ;
    }, [includePendingItems, matchCase, metricGraph, metrics, scopeFilter, scopesTable, view.properties, view.scopeTypes]);

    const updateScopesTable = React.useCallback((selection?: IScope[]) => {
        if (scopesTable?.renderCount() > 0 && selectedMetricsSource !== "scopesTable") {
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
    }, [scopesTable, selectedMetricsSource]);

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

    const onLineageSelectionChange = React.useCallback((lineageSelection: string) => {
        pushSelectionUrl(parentUrl, lineageSelection, selection);
    }, [parentUrl, selection]);

    const onSelectionChange = React.useCallback((selection: string[]) => {
        setSelectedMetricsSource("metricGraphWidget");
        pushSelectionUrl(parentUrl, lineageSelection, selection);
    }, [lineageSelection, parentUrl]);

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
                                    <TooltipHost content={nlsHPCC.FilterMetricsTooltip}>
                                        <SearchBox value={scopeFilter} onChange={onChangeScopeFilter} iconProps={filterIcon} placeholder={nlsHPCC.Filter} />
                                    </TooltipHost>
                                </Stack.Item>
                                <ToggleButton appearance="subtle" icon={matchCase ? <TextCaseTitleFilled /> : <TextCaseTitleRegular />} title={nlsHPCC.MatchCase} checked={matchCase} onClick={() => { setMatchCase(!matchCase); }} />
                            </Stack>}
                            main={<AutosizeHpccJSComponent widget={scopesTable} ></AutosizeHpccJSComponent>}
                        />
                    </DockPanelItem>
                    <DockPanelItem key="metricsSql" title={nlsHPCC.MetricsSQL} location="tab-after" relativeTo="scopesTable">
                        <MetricsSQL defaultSql={view.sql} scopes={metrics} onSelectionChanged={selection => scopesSelectionChanged("scopesSqlTable", lineageSelection, selection)}></MetricsSQL>
                    </DockPanelItem>
                    <DockPanelItem key="metricGraph" title={nlsHPCC.Graph} location="split-right" relativeTo="scopesTable" >
                        <MetricsGraph
                            metricGraphData={metricGraphData}
                            lineageSelection={lineageSelection}
                            selection={selection}
                            selectedMetricsSource={selectedMetricsSource}
                            status={status}
                            onLineageSelectionChange={onLineageSelectionChange}
                            onSelectionChange={onSelectionChange}>
                        </MetricsGraph>
                    </DockPanelItem>
                    <DockPanelItem key="propsTable" title={nlsHPCC.Properties} location="split-bottom" relativeTo="scopesTable" >
                        <MetricsPropertiesTables scopesTableColumns={scopesTable.columns()} scopes={selectedMetrics}></MetricsPropertiesTables>
                    </DockPanelItem>
                    <DockPanelItem key="propsTable2" title={nlsHPCC.CrossTab} location="tab-after" relativeTo="propsTable" >
                        <AutosizeHpccJSComponent widget={crossTabTable}></AutosizeHpccJSComponent>
                    </DockPanelItem>
                </DockPanel>
                <MetricsOptions show={showMetricOptions} setShow={setShowMetricOptionsHook} />
            </ErrorBoundary >
        }
    />;
};
