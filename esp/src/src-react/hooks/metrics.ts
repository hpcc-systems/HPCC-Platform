import * as React from "react";
import { useConst, useForceUpdate } from "@fluentui/react-hooks";
import { WsWorkunits, WorkunitsService, IScope } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { userKeyValStore } from "src/KeyValStore";
import { DockPanelLayout } from "../layouts/DockPanel";
import { singletonDebounce } from "../util/throttle";
import { signal, useSignal } from "../hooks/signal";
import { useWorkunit } from "./workunit";
import { useQuery } from "./query";
import { useCounter } from "./util";
import { useUserStore } from "./store";

const logger = scopedLogger("src-react/hooks/metrics.ts");

const METRIC_OPTIONS_2 = "MetricOptions-2";
const METRIC_OPTIONS_3 = "MetricOptions-3";

export function resetMetricsViews() {
    const store = userKeyValStore();
    return Promise.all([store?.delete(METRIC_OPTIONS_2), store?.delete(METRIC_OPTIONS_3)]);
}

export interface MetricsView {
    scopeTypes: string[];
    properties: string[];
    ignoreGlobalStoreOutEdges: boolean;
    subgraphTpl;
    activityTpl;
    edgeTpl;
    sql: string;
    layout?: DockPanelLayout;
    showTimeline: boolean;
}
export type StringMetricsViewMap = { [id: string]: MetricsView };

interface UserMetricsView {
    viewId: string;
    views: StringMetricsViewMap;
}

const DefaultMetricsViews: StringMetricsViewMap = {
    Default: {
        scopeTypes: ["graph", "subgraph", "activity", "operation", "workflow"],
        properties: ["CostExecute", "TimeElapsed"],
        ignoreGlobalStoreOutEdges: true,
        subgraphTpl: "%id% - %TimeElapsed%",
        activityTpl: "%Label%",
        edgeTpl: "%Label%\n%NumRowsProcessed%\n%SkewMinRowsProcessed% / %SkewMaxRowsProcessed%",
        sql: `\
SELECT type, name, CostExecute, TimeElapsed, id
    FROM metrics`,
        layout: undefined,
        showTimeline: true
    },
    Graphs: {
        scopeTypes: ["graph", "subgraph"],
        properties: ["CostExecute", "TimeElapsed"],
        ignoreGlobalStoreOutEdges: true,
        subgraphTpl: "%id% - %TimeElapsed%",
        activityTpl: "%Label%",
        edgeTpl: "%Label%\n%NumRowsProcessed%\n%SkewMinRowsProcessed% / %SkewMaxRowsProcessed%",
        sql: `\
SELECT type, name, CostExecute, TimeElapsed, id
    FROM metrics
    WHERE type = 'graph' OR type = 'subgraph'`,
        layout: undefined,
        showTimeline: true
    },
    Activities: {
        scopeTypes: ["activity"],
        properties: ["TimeMaxLocalExecute", "TimeMaxTotalExecute"],
        ignoreGlobalStoreOutEdges: true,
        subgraphTpl: "%id% - %TimeElapsed%",
        activityTpl: "%Label%",
        edgeTpl: "%Label%\n%NumRowsProcessed%\n%SkewMinRowsProcessed% / %SkewMaxRowsProcessed%",
        sql: `\
SELECT type, name, TimeLocalExecute, TimeTotalExecute, id
    FROM metrics
    WHERE type = 'activity'`,
        layout: undefined,
        showTimeline: true
    },
    Peaks: {
        scopeTypes: ["subgraph"],
        properties: ["NodeMaxPeakMemory", "NodeMaxPeakRowMemory", "NodeMinPeakMemory", "NodeMinPeakRowMemory", "SizeAvgPeakMemory", "SizeAvgPeakRowMemory", "SizeDeltaPeakMemory", "SizeDeltaPeakRowMemory", "SizeMaxPeakMemory", "SizeMaxPeakRowMemory", "SizeMinPeakMemory", "SizeMinPeakRowMemory", "SizePeakMemory", "SizeStdDevPeakMemory", "SizeStdDevPeakRowMemory", "SkewMaxPeakMemory", "SkewMaxPeakRowMemory", "SkewMinPeakMemory", "SkewMinPeakRowMemory"],
        ignoreGlobalStoreOutEdges: true,
        subgraphTpl: "%id% - %TimeElapsed%",
        activityTpl: "%Label%",
        edgeTpl: "%Label%\n%NumRowsProcessed%\n%SkewMinRowsProcessed% / %SkewMaxRowsProcessed%",
        sql: `\
SELECT type, name, NodeMaxPeakMemory, NodeMaxPeakRowMemory, NodeMinPeakMemory, NodeMinPeakRowMemory, SizeAvgPeakMemory, SizeAvgPeakRowMemory, SizeDeltaPeakMemory, SizeDeltaPeakRowMemory, SizeMaxPeakMemory, SizeMaxPeakRowMemory, SizeMinPeakMemory, SizeMinPeakRowMemory, SizePeakMemory, SizeStdDevPeakMemory, SizeStdDevPeakRowMemory, SkewMaxPeakMemory, SkewMaxPeakRowMemory, SkewMinPeakMemory, SkewMinPeakRowMemory, id
    FROM metrics
    WHERE type = 'subgraph'`,
        layout: undefined,
        showTimeline: true
    }
};

// const _defaultUserViews = JSON.stringify({ viewId: _viewId.value, views: _views.value });

export function clone<T>(_: T): T {
    return JSON.parse(JSON.stringify(_));
}

// function checkLayout(options: MetricsView): boolean {
//     if (options?.layout && !options?.layout?.["main"]) {
//         delete options.layout;
//     }
//     return !!options?.layout;
// }

export interface useMetricsViewsResult {
    viewIds: string[];
    viewId: string;
    setViewId: (id: string, forceRefresh?: boolean) => void;
    view: MetricsView;
    addView: (label: string, view: Partial<MetricsView>) => void;
    updateView: (view: Partial<MetricsView>, forceRefresh?: boolean) => void;
    resetView: (forceRefresh?: boolean) => void;
    save: () => void;
}

const defaultUserMetricViews = JSON.stringify({ viewId: "Default", views: DefaultMetricsViews });

const _loaded = signal(false);
const _origViews = signal<StringMetricsViewMap>(clone(DefaultMetricsViews));
const _views = signal<StringMetricsViewMap>(clone(DefaultMetricsViews));
const _viewIds = signal<string[]>(Object.keys(_views.get()));
const _viewId = signal<string>(_viewIds.get()[0]);
const _view = signal<MetricsView>(_views.get()[_viewId.get()]);

export function useMetricsViews(): useMetricsViewsResult {

    const loaded = useSignal(_loaded);
    const origViews = useSignal(_origViews);
    const views = useSignal(_views);
    const viewIds = useSignal(_viewIds);
    const viewId = useSignal(_viewId);
    const view = useSignal(_view);

    const refresh = useForceUpdate();

    const setViewId = React.useCallback((id: string) => {
        _viewId.set(id);
    }, []);

    const [metricViewStr, setMetricViewStr, _resetMetricsViewStr] = useUserStore<string>(METRIC_OPTIONS_3, defaultUserMetricViews);
    React.useEffect(() => {
        if (metricViewStr && !loaded) {
            try {
                const userView: UserMetricsView = JSON.parse(metricViewStr);
                _origViews.set(clone(userView.views));
                _views.set(userView.views);
                _viewIds.set(Object.keys(userView.views));
                setViewId(userView.viewId);
                _loaded.set(true);
            } catch (e) {
                logger.error(e);
                const def = clone(DefaultMetricsViews);
                const keys = Object.keys(def);
                _views.set(def);
                _viewIds.set(keys);
                setViewId(keys[0]);
            }
        }
    }, [loaded, metricViewStr, setViewId]);

    React.useEffect(() => {
        _view.set(views[viewId]);
    }, [viewId, views]);

    const addView = React.useCallback((label: string, _: Partial<MetricsView>) => {
        origViews[label] = clone(view);
        for (const key in _) {
            origViews[label] = _[key];
        }
        setViewId(label);
        refresh();
    }, [origViews, refresh, setViewId, view]);

    const updateView = React.useCallback((_: Partial<MetricsView>, forceRefresh = false) => {
        for (const key in _) {
            view[key] = _[key];
        }
        if (forceRefresh) {
            refresh();
        }
    }, [refresh, view]);

    const resetView = React.useCallback((forceRefresh = false) => {
        const def = DefaultMetricsViews[viewId] ?? DefaultMetricsViews["Default"];
        for (const key in def) {
            view[key] = def[key];
        }
        if (forceRefresh) {
            refresh();
        }
    }, [refresh, view, viewId]);

    const save = React.useCallback(() => {
        return setMetricViewStr(JSON.stringify({ viewId, views }));
    }, [viewId, views, setMetricViewStr]);

    return {
        viewIds,
        viewId,
        setViewId,
        view,
        addView,
        updateView,
        resetView,
        save
    };
}

let wuDetailsMetaResponse: Promise<WsWorkunits.WUDetailsMetaResponse>;

export function useMetricMeta(): [string[], string[]] {

    const service = useConst(() => new WorkunitsService({ baseUrl: "" }));
    const [scopeTypes, setScopeTypes] = React.useState<string[]>([]);
    const [properties, setProperties] = React.useState<string[]>([]);

    React.useEffect(() => {
        if (!wuDetailsMetaResponse && service) {
            wuDetailsMetaResponse = service.WUDetailsMeta({});
        }
        wuDetailsMetaResponse?.then(response => {
            setScopeTypes(response?.ScopeTypes?.ScopeType || []);
            setProperties((response?.Properties?.Property.map(p => p.Name) || []).sort());
        });
    }, [service]);

    return [scopeTypes, properties];
}

export enum FetchStatus {
    UNKNOWN,
    STARTED,
    COMPLETE
}

const scopeFilterDefault: Partial<WsWorkunits.ScopeFilter> = {
    MaxDepth: 999999,
    ScopeTypes: []
};

const nestedFilterDefault: WsWorkunits.NestedFilter = {
    Depth: 0,
    ScopeTypes: []
};

export interface useMetricsResult {
    metrics: IScope[];
    columns: { [id: string]: any };
    activities: WsWorkunits.Activity2[];
    properties: WsWorkunits.Property2[];
    measures: string[];
    scopeTypes: string[];
    status: FetchStatus;
    refresh: () => void;
}

export interface IScopeEx extends IScope {
    __exceptions?: WsWorkunits.ECLException[],
}

export function useWorkunitMetrics(
    wuid: string,
    scopeFilter: Partial<WsWorkunits.ScopeFilter> = scopeFilterDefault,
    nestedFilter: WsWorkunits.NestedFilter = nestedFilterDefault
): useMetricsResult {

    const { workunit, state } = useWorkunit(wuid);
    const [data, setData] = React.useState<IScopeEx[]>([]);
    const [columns, setColumns] = React.useState<{ [id: string]: any }>([]);
    const [activities, setActivities] = React.useState<WsWorkunits.Activity2[]>([]);
    const [properties, setProperties] = React.useState<WsWorkunits.Property2[]>([]);
    const [measures, setMeasures] = React.useState<string[]>([]);
    const [scopeTypes, setScopeTypes] = React.useState<string[]>([]);
    const [status, setStatus] = React.useState<FetchStatus>(FetchStatus.COMPLETE);
    const [count, increment] = useCounter();

    React.useEffect(() => {
        if (wuid && workunit) {
            const fetchInfo = singletonDebounce(workunit, "fetchInfo");
            const fetchDetailsNormalized = singletonDebounce(workunit, "fetchDetailsNormalized");
            setStatus(FetchStatus.STARTED);
            Promise.all([
                fetchInfo({ IncludeExceptions: true }),
                fetchDetailsNormalized({
                    ScopeFilter: scopeFilter,
                    NestedFilter: nestedFilter,
                    PropertiesToReturn: {
                        AllScopes: true,
                        AllAttributes: true,
                        AllProperties: true,
                        AllNotes: true,
                        AllStatistics: true,
                        AllHints: true
                    },
                    ScopeOptions: {
                        IncludeId: true,
                        IncludeScope: true,
                        IncludeScopeType: true,
                        IncludeMatchedScopesInResults: true
                    },
                    PropertyOptions: {
                        IncludeName: true,
                        IncludeRawValue: true,
                        IncludeFormatted: true,
                        IncludeMeasure: true,
                        IncludeCreator: false,
                        IncludeCreatorType: false
                    }
                })
            ]).then(([info, response]) => {
                const exceptionsMap: { [scope: string]: WsWorkunits.ECLException[] } = {};
                for (const exception of info?.Workunit?.Exceptions?.ECLException ?? []) {
                    if (exception.Scope) {
                        if (!exceptionsMap[exception.Scope]) {
                            exceptionsMap[exception.Scope] = [];
                        }
                        exceptionsMap[exception.Scope].push(exception);
                    }
                }
                setData(response?.data.map(row => {
                    if (exceptionsMap[row.name]) {
                        row.__exceptions = exceptionsMap[row.name];
                    }
                    return row;
                }));
                setColumns(response?.columns);
                setActivities(response?.meta?.Activities?.Activity ?? []);
                setProperties(response?.meta?.Properties?.Property ?? []);
                setMeasures(response?.meta?.Measures?.Measure ?? []);
                setScopeTypes(response?.meta?.ScopeTypes?.ScopeType ?? []);
            }).catch(e => {
                logger.error(e);
            }).finally(() => {
                setStatus(FetchStatus.COMPLETE);
            });
        }
    }, [workunit, state, count, scopeFilter, nestedFilter, wuid]);

    return { metrics: data, columns, activities, properties, measures, scopeTypes, status, refresh: increment };
}

export function useQueryMetrics(
    querySet: string,
    queryId: string,
    scopeFilter: Partial<WsWorkunits.ScopeFilter> = scopeFilterDefault,
    nestedFilter: WsWorkunits.NestedFilter = nestedFilterDefault
): useMetricsResult {

    const [query, state, _refresh] = useQuery(querySet, queryId);
    const [data, setData] = React.useState<IScope[]>([]);
    const [columns, setColumns] = React.useState<{ [id: string]: any }>([]);
    const [activities, setActivities] = React.useState<WsWorkunits.Activity2[]>([]);
    const [properties, setProperties] = React.useState<WsWorkunits.Property2[]>([]);
    const [measures, setMeasures] = React.useState<string[]>([]);
    const [scopeTypes, setScopeTypes] = React.useState<string[]>([]);
    const [status, setStatus] = React.useState<FetchStatus>(FetchStatus.COMPLETE);
    const [count, increment] = useCounter();

    React.useEffect(() => {
        if (querySet && queryId && query) {
            const fetchDetailsNormalized = singletonDebounce(query, "fetchDetailsNormalized");
            setStatus(FetchStatus.STARTED);
            fetchDetailsNormalized({
                ScopeFilter: scopeFilter,
                NestedFilter: nestedFilter,
                PropertiesToReturn: {
                    AllScopes: true,
                    AllAttributes: true,
                    AllProperties: true,
                    AllNotes: true,
                    AllStatistics: true,
                    AllHints: true
                },
                ScopeOptions: {
                    IncludeId: true,
                    IncludeScope: true,
                    IncludeScopeType: true,
                    IncludeMatchedScopesInResults: true
                },
                PropertyOptions: {
                    IncludeName: true,
                    IncludeRawValue: true,
                    IncludeFormatted: true,
                    IncludeMeasure: true,
                    IncludeCreator: false,
                    IncludeCreatorType: false
                }
            }).then(response => {
                setData(response?.data);
                setColumns(response?.columns);
                setActivities(response?.meta?.Activities?.Activity || []);
                setProperties(response?.meta?.Properties?.Property || []);
                setMeasures(response?.meta?.Measures?.Measure || []);
                setScopeTypes(response?.meta?.ScopeTypes?.ScopeType || []);
            }).catch(e => {
                logger.error(e);
            }).finally(() => {
                setStatus(FetchStatus.COMPLETE);
            });
        }
    }, [query, state, count, scopeFilter, nestedFilter, querySet, queryId]);

    return { metrics: data, columns, activities, properties, measures, scopeTypes, status, refresh: increment };
}

export function useWUQueryMetrics(
    wuid: string,
    querySet: string,
    queryId: string,
    scopeFilter: Partial<WsWorkunits.ScopeFilter> = scopeFilterDefault,
    nestedFilter: WsWorkunits.NestedFilter = nestedFilterDefault
): useMetricsResult {
    const isQuery = querySet && queryId;
    const wuMetrics = useWorkunitMetrics(isQuery ? "" : wuid, scopeFilter, nestedFilter);
    const queryMetrics = useQueryMetrics(isQuery ? querySet : "", isQuery ? queryId : "", scopeFilter, nestedFilter);
    return isQuery ? { ...queryMetrics } : { ...wuMetrics };
}
