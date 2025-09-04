import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { type Activity, SMCService, Workunit, DFUWorkunit, type WsSMC } from "@hpcc-js/comms";
import { useActivity } from "./activity";

const serverTypeFromClusterType = (clusterType?: number): string => {
    switch (clusterType) {
        case 1: return "HThorServer";
        case 2: return "RoxieServer";
        case 3: return "ThorMaster";
        default: return "";
    }
};

export interface ActiveWorkunitEx extends WsSMC.ActiveWorkunit {
    isDFU: boolean;
    isPaused: boolean;
}

export interface ServerJobQueue {
    kind: "ServerJobQueue" | "TargetCluster";
    title: string;
    paused: boolean;
    status: string;
    statusDetails: string;
    serverJobQueue?: WsSMC.ServerJobQueue;
    targetCluster?: WsSMC.TargetCluster;
    workunits: ActiveWorkunitEx[];
}

function calcServerJobQueue(activity: Activity): ServerJobQueue[] {
    const retVal: ServerJobQueue[] = [];

    const sjq: any[] = activity?.ServerJobQueues?.ServerJobQueue ?? [];
    sjq.forEach(q => {
        const item: ServerJobQueue = {
            kind: "ServerJobQueue",
            title: q.ServerType + (q.ServerName ? ` - ${q.ServerName}` : ""),
            paused: q.QueueStatus === "paused",
            status: q.QueueStatus ?? "",
            statusDetails: q.StatusDetails ?? "",
            serverJobQueue: q,
            workunits: []
        };
        retVal.push(item);
    });

    const pushClusters = (tcList: any[] | undefined, clusterType: number) => {
        (tcList ?? []).forEach(tc => {
            const item: ServerJobQueue = {
                kind: "TargetCluster",
                title: serverTypeFromClusterType(tc.ClusterType) + (tc.ClusterName ? ` - ${tc.ClusterName}` : ""),
                paused: tc.targetCluster?.ClusterStatus === 1 || tc.ClusterStatus === 2,
                status: tc.QueueStatus ?? "",
                statusDetails: tc.StatusDetails ?? "",
                targetCluster: tc,
                workunits: []
            };
            if (item.targetCluster && item.targetCluster.ClusterType === undefined) {
                (item.targetCluster as any).ClusterType = clusterType;
            }
            retVal.push(item);
        });
    };

    pushClusters(activity?.HThorClusterList?.TargetCluster, 1);
    pushClusters(activity?.RoxieClusterList?.TargetCluster, 2);
    pushClusters(activity?.ThorClusterList?.TargetCluster, 3);

    const queueNameMap = new Map<string, ServerJobQueue>();
    const instanceMap = new Map<string, ServerJobQueue>();
    const targetClusterMap = new Map<string, ServerJobQueue>();

    for (const r of retVal) {
        if (r.targetCluster?.ClusterName ?? r.serverJobQueue?.ServerName) {
            instanceMap.set(r.targetCluster?.ClusterName ?? r.serverJobQueue?.ServerName, r);
        }
        if (r.targetCluster?.ClusterName) {
            instanceMap.set(r.targetCluster?.ClusterName, r);
        }
        if (r.targetCluster?.QueueName) {
            queueNameMap.set(r.targetCluster?.QueueName, r);
        }
    }

    for (const item of activity?.Running?.ActiveWorkunit ?? []) {
        if (item.Wuid[0] === "W" || item.Wuid[0] === "D") {
            const found: ServerJobQueue | undefined =
                queueNameMap.get(item.QueueName) ??
                instanceMap.get(item.Instance) ??
                instanceMap.get(item.QueueName) ??
                instanceMap.get(item.TargetClusterName) ??
                targetClusterMap.get(item.ClusterName);

            if (found) {
                found.workunits.push({
                    isDFU: item.Wuid[0] === "D",
                    isPaused: item.IsPausing || item.State.includes("pause"),
                    ...item
                });
            } else {
                console.warn("Unknown Q:", item);
            }
        }
    }
    return retVal;
}

export function useServerJobQueues() {

    const { activity, lastUpdate, refresh } = useActivity();
    const [queues, setQueues] = React.useState<ServerJobQueue[]>([]);
    const smc = useConst(() => new SMCService({ baseUrl: "" }));

    const delayedRefresh = React.useCallback((delay: number = 500) => {
        setTimeout(() => {
            refresh();
        }, delay);
    }, [refresh]);

    React.useEffect(() => {
        setQueues(calcServerJobQueue(activity));
    }, [activity, lastUpdate]);

    const pause = React.useCallback(async (item: ServerJobQueue) => {
        try {
            if (item.kind === "TargetCluster") {
                await smc.PauseQueue({
                    Cluster: item.targetCluster?.ClusterName,
                    QueueName: item.targetCluster?.QueueName,
                    ServerType: serverTypeFromClusterType(item.targetCluster?.ClusterType),
                    Comment: undefined,
                    NetworkAddress: undefined,
                    Port: undefined
                });
            } else {
                await smc.PauseQueue({
                    Cluster: item.serverJobQueue?.ServerName,
                    QueueName: item.serverJobQueue?.QueueName,
                    ServerType: item.serverJobQueue?.ServerType,
                    Comment: undefined,
                    NetworkAddress: item.serverJobQueue?.NetworkAddress,
                    Port: item.serverJobQueue?.Port
                });
            }
        } finally {
            delayedRefresh();
        }
    }, [smc, delayedRefresh]);

    const resume = React.useCallback(async (item: ServerJobQueue) => {
        try {
            if (item.kind === "TargetCluster") {
                const serverType = serverTypeFromClusterType(item.targetCluster?.ClusterType);
                await smc.ResumeQueue({
                    Cluster: item.targetCluster?.ClusterName,
                    QueueName: item.targetCluster?.QueueName,
                    ServerType: serverType,
                    Comment: undefined,
                    NetworkAddress: undefined,
                    Port: undefined
                });
            } else {
                await smc.ResumeQueue({
                    Cluster: item.serverJobQueue?.ServerName,
                    QueueName: item.serverJobQueue?.QueueName,
                    ServerType: item.serverJobQueue?.ServerType,
                    Comment: undefined,
                    NetworkAddress: item.serverJobQueue?.NetworkAddress,
                    Port: item.serverJobQueue?.Port
                });
            }
        } finally {
            delayedRefresh();
        }
    }, [smc, delayedRefresh]);

    const clear = React.useCallback(async (item: ServerJobQueue) => {
        try {
            if (item.kind === "TargetCluster") {
                Promise.all(item.workunits.map(wu => {
                    if (wu.isDFU) {
                        return DFUWorkunit.attach({ baseUrl: "" }, wu.Wuid).abort();
                    } else {
                        return Workunit.attach({ baseUrl: "" }, wu.Wuid).abort();
                    }
                })).then(() => {
                    delayedRefresh();
                });
            } else {
                await smc.ClearQueue({
                    Cluster: item.serverJobQueue?.ServerName,
                    QueueName: item.serverJobQueue?.QueueName,
                    ServerType: item.serverJobQueue?.ServerType,
                    Comment: undefined,
                    NetworkAddress: item.serverJobQueue?.NetworkAddress,
                    Port: item.serverJobQueue?.Port
                });
            }
        } finally {
            delayedRefresh();
        }
    }, [smc, delayedRefresh]);

    const setPriority = React.useCallback(async (wu: ActiveWorkunitEx, priority: "high" | "normal" | "low") => {
        try {
            await smc.SetJobPriority({
                QueueName: wu.QueueName,
                Wuid: wu.Wuid,
                Priority: priority,
                SMCJobs: undefined
            });
        } finally {
            delayedRefresh();
        }
    }, [smc, delayedRefresh]);

    const moveTop = React.useCallback(async (wu: ActiveWorkunitEx) => {
        try {
            await smc.MoveJobFront({
                QueueName: wu.QueueName,
                Wuid: wu.Wuid,
                ClusterType: undefined,
                Cluster: undefined
            });
        } finally {
            delayedRefresh();
        }
    }, [smc, delayedRefresh]);

    const moveUp = React.useCallback(async (wu: ActiveWorkunitEx) => {
        try {
            await smc.MoveJobUp({
                QueueName: wu.QueueName,
                Wuid: wu.Wuid,
                ClusterType: undefined,
                Cluster: undefined
            });
        } finally {
            delayedRefresh();
        }
    }, [smc, delayedRefresh]);

    const moveDown = React.useCallback(async (wu: ActiveWorkunitEx) => {
        try {
            await smc.MoveJobDown({
                QueueName: wu.QueueName,
                Wuid: wu.Wuid,
                ClusterType: undefined,
                Cluster: undefined
            });
        } finally {
            delayedRefresh();
        }
    }, [smc, delayedRefresh]);

    const moveBottom = React.useCallback(async (wu: ActiveWorkunitEx) => {
        try {
            await smc.MoveJobBack({
                QueueName: wu.QueueName,
                Wuid: wu.Wuid,
                ClusterType: undefined,
                Cluster: undefined
            });
        } finally {
            delayedRefresh();
        }
    }, [smc, delayedRefresh]);


    const wuPause = React.useCallback(async (wu: ActiveWorkunitEx, now: boolean) => {
        try {
            if (wu.isDFU) {
            } else if (now) {
                await Workunit.attach({ baseUrl: "" }, wu.Wuid).pauseNow();
            } else {
                await Workunit.attach({ baseUrl: "" }, wu.Wuid).pause();
            }
        } finally {
            delayedRefresh();
        }
    }, [delayedRefresh]);

    const wuResume = React.useCallback(async (wu: ActiveWorkunitEx) => {
        try {
            if (wu.isDFU) {
            } else {
                await Workunit.attach({ baseUrl: "" }, wu.Wuid).resume();
            }
        } finally {
            delayedRefresh();
        }
    }, [delayedRefresh]);

    return { queues, lastUpdate, refresh, pause, resume, clear, setPriority, moveTop, moveUp, moveDown, moveBottom, wuPause, wuResume };
}
