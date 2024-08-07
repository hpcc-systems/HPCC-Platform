import * as React from "react";
import { useConst, useForceUpdate } from "@fluentui/react-hooks";
import { WsWorkunits, WorkunitsService, IScope } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { userKeyValStore } from "src/KeyValStore";
import { useWorkunit } from "./workunit";
import { useQuery } from "./query";
import { useCounter } from "./util";

const logger = scopedLogger("src-react/hooks/metrics.ts");

const METRIC_OPTIONS_VERSION = 2;
const METRIC_OPTIONS_KEY = `MetricOptions-${METRIC_OPTIONS_VERSION}`;

export function resetMetricsViews() {
    const store = userKeyValStore();
    return store?.delete(METRIC_OPTIONS_KEY);
}

export interface MetricsOptions {
    scopeTypes: string[];
    properties: string[];
    ignoreGlobalStoreOutEdges: boolean;
    subgraphTpl;
    activityTpl;
    edgeTpl;
    sql: string;
    layout?: object;
    showTimeline: boolean;
}

const defaults: MetricsOptions = {
    scopeTypes: ["graph", "subgraph", "activity", "operation", "workflow"],
    properties: ["TimeElapsed"],
    ignoreGlobalStoreOutEdges: true,
    subgraphTpl: "%id% - %TimeElapsed%",
    activityTpl: "%Label%",
    edgeTpl: "%Label%\n%NumRowsProcessed%\n%SkewMinRowsProcessed% / %SkewMaxRowsProcessed%",
    sql: "SELECT type, name, TimeElapsed, id\n    FROM metrics\n    WHERE TimeElapsed IS NOT NULL",
    layout: undefined,
    showTimeline: true
};

const options: MetricsOptions = { ...defaults };

function checkLayout(options: MetricsOptions): boolean {
    if (options?.layout && !options?.layout?.["main"]) {
        delete options.layout;
    }
    return !!options?.layout;
}

export function useMetricsOptions(): [MetricsOptions, (opts: MetricsOptions) => void, () => void, (toDefaults?: boolean) => void] {

    const store = useConst(() => userKeyValStore());
    const refresh = useForceUpdate();

    const setOptions = React.useCallback((opts: MetricsOptions) => {
        for (const key in opts) {
            options[key] = opts[key];
        }
        refresh();
    }, [refresh]);

    const save = React.useCallback(() => {
        if (checkLayout(options)) {
            store?.set(METRIC_OPTIONS_KEY, JSON.stringify(options), true);
        }
    }, [store]);

    const reset = React.useCallback((toDefaults: boolean = false) => {
        if (toDefaults) {
            setOptions({ ...defaults });
        } else {
            store?.get(METRIC_OPTIONS_KEY).then(opts => {
                const options = JSON.parse(opts);
                checkLayout(options);
                setOptions({ ...defaults, ...options });
            });
        }
    }, [setOptions, store]);

    React.useEffect(() => {
        reset();
        const handle = store?.monitor(() => {
            reset();
        });

        return () => handle?.release();
    }, [reset, store]);

    return [options, setOptions, save, reset];
}

export function useMetricMeta(): [string[], string[]] {

    const service = useConst(() => new WorkunitsService({ baseUrl: "" }));
    const [scopeTypes, setScopeTypes] = React.useState<string[]>([]);
    const [properties, setProperties] = React.useState<string[]>([]);

    React.useEffect(() => {
        service?.WUDetailsMeta({}).then(response => {
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

export function useWorkunitMetrics(
    wuid: string,
    scopeFilter: Partial<WsWorkunits.ScopeFilter> = scopeFilterDefault,
    nestedFilter: WsWorkunits.NestedFilter = nestedFilterDefault
): [IScope[], { [id: string]: any }, WsWorkunits.Activity2[], WsWorkunits.Property2[], string[], string[], FetchStatus, () => void] {

    const [workunit, state] = useWorkunit(wuid);
    const [data, setData] = React.useState<IScope[]>([]);
    const [columns, setColumns] = React.useState<{ [id: string]: any }>([]);
    const [activities, setActivities] = React.useState<WsWorkunits.Activity2[]>([]);
    const [properties, setProperties] = React.useState<WsWorkunits.Property2[]>([]);
    const [measures, setMeasures] = React.useState<string[]>([]);
    const [scopeTypes, setScopeTypes] = React.useState<string[]>([]);
    const [status, setStatus] = React.useState<FetchStatus>(FetchStatus.COMPLETE);
    const [count, increment] = useCounter();

    React.useEffect(() => {
        setStatus(FetchStatus.STARTED);
        workunit?.fetchDetailsNormalized({
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
    }, [workunit, state, count, scopeFilter, nestedFilter]);

    return [data, columns, activities, properties, measures, scopeTypes, status, increment];
}

export function useQueryMetrics(
    querySet: string,
    queryId: string,
    scopeFilter: Partial<WsWorkunits.ScopeFilter> = scopeFilterDefault,
    nestedFilter: WsWorkunits.NestedFilter = nestedFilterDefault
): [IScope[], { [id: string]: any }, WsWorkunits.Activity2[], WsWorkunits.Property2[], string[], string[], FetchStatus, () => void] {

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
        setStatus(FetchStatus.STARTED);
        query?.fetchDetailsNormalized({
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
    }, [query, state, count, scopeFilter, nestedFilter]);

    return [data, columns, activities, properties, measures, scopeTypes, status, increment];
}

export function useWUQueryMetrics(
    wuid: string,
    querySet: string,
    queryId: string,
    scopeFilter: Partial<WsWorkunits.ScopeFilter> = scopeFilterDefault,
    nestedFilter: WsWorkunits.NestedFilter = nestedFilterDefault
): [IScope[], { [id: string]: any }, WsWorkunits.Activity2[], WsWorkunits.Property2[], string[], string[], FetchStatus, () => void] {
    const wuMetrics = useWorkunitMetrics(wuid, scopeFilter, nestedFilter);
    const queryMetrics = useQueryMetrics(querySet, queryId, scopeFilter, nestedFilter);
    return querySet && queryId ? [...queryMetrics] : [...wuMetrics];
}
