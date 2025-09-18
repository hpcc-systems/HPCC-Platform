import { useCallback, useEffect, useMemo, useState } from "react";
import { MachineService, WsDFUXRef, WsMachineEx } from "@hpcc-js/comms";

export interface DirectoryEx {
    Cluster: string;
    Num: number;
    Name: string;
    MaxSize: number;
    MaxIP: string;
    MinSize: number;
    MinIP: string;
    Size: number;
    PositiveSkew: string;
}

export interface XREFDirectories {
    nodes: WsDFUXRef.XRefNode[];
    directories: DirectoryEx[];
}

export interface UseAsyncResult<T> {
    data?: T;
    loading: boolean;
    error?: unknown;
    refresh: () => void;
}

function useMachineService(): MachineService {
    return useMemo(() => new MachineService({ baseUrl: "", timeoutSecs: 360 }), []);
}

export interface useTargetClusterUsageExOptions {
    bypassCachedResult?: boolean;
    refreshIntervalMs?: number;
}

export function useTargetClusterUsageEx(targetCluster?: string, options?: useTargetClusterUsageExOptions): UseAsyncResult<WsMachineEx.TargetClusterUsage[]> {
    const machineService = useMachineService();
    const [data, setData] = useState<WsMachineEx.TargetClusterUsage[] | undefined>();
    const [loading, setLoading] = useState<boolean>(false);
    const [error, setError] = useState<unknown>();

    const fetchData = useCallback(() => {
        let cancelled = false;
        setLoading(true);
        setError(undefined);
        machineService
            .GetTargetClusterUsageEx(
                targetCluster !== undefined ? [targetCluster] : undefined,
                options?.bypassCachedResult
            )
            .then((resp) => {
                if (!cancelled) setData(resp);
            })
            .catch((e) => {
                if (!cancelled) setError(e);
            })
            .finally(() => {
                if (!cancelled) setLoading(false);
            });
        return () => {
            cancelled = true;
        };
    }, [machineService, targetCluster, options?.bypassCachedResult]);

    useEffect(() => {
        const cancel = fetchData();
        return cancel;
    }, [fetchData]);

    useEffect(() => {
        if (!options?.refreshIntervalMs) return;
        const id = setInterval(() => {
            fetchData();
        }, options.refreshIntervalMs);
        return () => clearInterval(id);
    }, [fetchData, options?.refreshIntervalMs]);

    return {
        data,
        loading,
        error,
        refresh: fetchData,
    };
}

const calcPct = (val: number, tot: number) => tot > 0 ? Math.round((val / tot) * 100) : 0;

export interface ClusterGaugeDatum {
    name: string;             // Cluster name
    value: number;            // 0..1 (max%)
    tick: number;             // 0..1 (mean%)
    tooltip?: string;         // ComponentUsagesDescription
    raw: WsMachineEx.TargetClusterUsage; // Raw server response
}

export interface HookState<T> {
    data: T;
    loading: boolean;
    error: unknown;
    refresh: () => void;
}

export function useAllClustersDiskUsage(bypassCachedResult: boolean = false): HookState<ClusterGaugeDatum[]> {
    const { data: raw, loading, error, refresh } = useTargetClusterUsageEx(undefined, { bypassCachedResult });

    const rows = useMemo<ClusterGaugeDatum[]>(() =>
        (raw ?? []).map(details => ({
            name: details.Name,
            value: ((details.max || 0) / 100),
            tick: ((details.mean || 0) / 100),
            tooltip: details.ComponentUsagesDescription,
            raw: details
        })), [raw]);

    return useMemo(() => ({ data: rows, loading, error: error ?? null, refresh }), [rows, loading, error, refresh]);
}

export interface ComponentAggregateDatum {
    name: string;             // Component/Disk name (du.Name)
    value: number;            // 0..1 (max%)
    tick: number;             // 0..1 (inUseMean / totalMean)
    stats: {
        rowCount: number;
        inUse: number;
        total: number;
        maxPct: number;       // 0..100
        inUseMean: number;    // average inUse across rows
        totalMean: number;    // average total across rows
    }
}

export function useClusterDiskUsage(cluster: string, bypassCachedResult: boolean = false): HookState<ComponentAggregateDatum[]> {
    const { data: raw, loading, error, refresh } = useTargetClusterUsageEx(cluster || undefined, { bypassCachedResult });

    const rows = useMemo<ComponentAggregateDatum[]>(() => {
        if (!cluster) return [];
        const details = (raw ?? []).find(d => d.Name === cluster);
        if (!details) return [];

        const agg: { [key: string]: { rowCount: number; inUse: number; total: number; maxPct: number; } } = {};
        details.ComponentUsages.forEach(cu => {
            cu.MachineUsages.forEach(mu => {
                mu.DiskUsages
                    .filter(du => !isNaN(du.InUse) || !isNaN(du.Total))
                    .forEach(du => {
                        if (!agg[du.Name]) {
                            agg[du.Name] = { rowCount: 0, inUse: 0, total: 0, maxPct: 0 };
                        }
                        const a = agg[du.Name];
                        a.rowCount++;
                        a.inUse += du.InUse;
                        a.total += du.Total;
                        const usagePct = calcPct(du.InUse, du.Total);
                        a.maxPct = a.maxPct < usagePct ? usagePct : a.maxPct;
                    });
            });
        });

        return Object.keys(agg).map(name => {
            const a = agg[name];
            const totalMean = a.rowCount > 0 ? a.total / a.rowCount : 0;
            const inUseMean = a.rowCount > 0 ? a.inUse / a.rowCount : 0;
            const tick = totalMean > 0 ? (inUseMean / totalMean) : 0;
            return {
                name,
                value: (a.maxPct / 100),
                tick,
                stats: {
                    rowCount: a.rowCount,
                    inUse: a.inUse,
                    total: a.total,
                    maxPct: a.maxPct,
                    inUseMean,
                    totalMean
                }
            };
        });
    }, [cluster, raw]);

    const safeLoading = cluster ? loading : false;
    const safeError = cluster ? (error ?? null) : null;

    return useMemo(() => ({ data: rows, loading: safeLoading, error: safeError, refresh }), [rows, safeLoading, safeError, refresh]);
}
