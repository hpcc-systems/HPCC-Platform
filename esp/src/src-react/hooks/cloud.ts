import * as React from "react";
import { CloudService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import type { V1Pod } from "@kubernetes/client-node";

import { useCounter } from "./workunit";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("../hooks/cloud.ts");

const service = new CloudService({ baseUrl: "" });

function formatAge(milliseconds: number): string {

    const seconds = Math.floor((milliseconds / 1000) % 60);
    const minutes = Math.floor((milliseconds / 1000 / 60) % 60);
    const hours = Math.floor((milliseconds / 1000 / 60 / 60) % 24);
    const days = Math.floor((milliseconds / 1000 / 60 / 60 / 24));
    if (days) {
        return `${days}d${hours}h`;
    } else if (hours) {
        return `${hours}h${minutes}m`;
    } else if (minutes) {
        return `${minutes}m`;
    } else if (seconds) {
        return `${seconds}s`;
    }
    return `${milliseconds}ms`;
}

export interface Pod {
    name: string;
    container: string;
    port: string;
    ready: string;
    status: string;
    restarts: number;
    age: string;
    payload: V1Pod;
}

export function usePods(): [Pod[], () => void] {

    const [retVal, setRetVal] = React.useState<Pod[]>([]);
    const [refreshTick, refresh] = useCounter();

    React.useEffect(() => {
        service.getPODs().then((pods: V1Pod[]) => {
            const now = Date.now();
            setRetVal(pods
                .filter(pod => {
                    const labels = pod?.metadata?.labels ?? {};
                    return labels.hasOwnProperty("app.kubernetes.io/part-of") && labels["app.kubernetes.io/part-of"] === "HPCC-Platform";
                })
                .map(pod => {
                    const started = new Date(pod.metadata?.creationTimestamp);
                    return {
                        name: pod.metadata.name,
                        container: pod.status?.containerStatuses?.reduce((prev, curr) => prev ? prev : curr.name, ""),
                        port: pod.spec?.containers?.reduce((prev, curr) => {
                            prev.push(curr.ports?.map(p => `${p.containerPort}/${p.protocol}`).join(", "));
                            return prev;
                        }, []).join(", "),
                        ready: `${pod.status?.containerStatuses?.reduce((prev, curr) => prev + (curr.ready ? 1 : 0), 0)}/${pod.status?.containerStatuses?.length}`,
                        status: pod.status?.phase,
                        restarts: pod.status?.containerStatuses?.reduce((prev, curr) => prev + curr.restartCount, 0),
                        age: formatAge(now - +started),
                        payload: pod
                    };
                })
            logger.error(nlsHPCC.PodsAccessError);
        });
    }, [refreshTick]);

    return [retVal, refresh];
}

export function useContainerNames(): [string[], () => void] {

    const [pods, refreshData] = usePods();
    const containers = React.useMemo(() => {
        return [...new Set(pods.map(pod => pod.container))];
    }, [pods]);

    return [containers, refreshData];
}