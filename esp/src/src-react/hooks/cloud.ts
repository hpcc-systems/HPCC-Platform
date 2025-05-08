import * as React from "react";
import { CloudService, WsCloud } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { useCounter } from "./util";

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

function extractPorts(pod: WsCloud.Pod): string[] {
    return pod.Ports?.Port?.reduce((acc, port) => {
        if (port.ContainerPort && port.Protocol) {
            acc.push(`${port.ContainerPort}/${port.Protocol}`);
        } else if (port.ContainerPort) {
            acc.push(`${port.ContainerPort}`);
        }
        return acc;
    }, []) ?? [];
}

interface PodEx extends WsCloud.Pod {
    ready: string;
    ports: string;
    age: string;
}

export function usePods(): [PodEx[], () => void] {

    const [retVal, setRetVal] = React.useState<PodEx[]>([]);
    const [refreshTick, refresh] = useCounter();

    React.useEffect(() => {
        service.getPODs().then(pods => {
            const now = Date.now();
            setRetVal(pods
                .map(pod => {
                    const started = new Date(pod.CreationTimestamp);
                    return {
                        ...pod,
                        ready: `${pod.ContainerReadyCount}/${pod.ContainerCount}`,
                        ports: extractPorts(pod).join(", "),
                        age: formatAge(now - +started),
                    };
                })
            );
        }).catch(err => {
            logger.error(`${nlsHPCC.PodsAccessError} (${err.message})`);
        });
    }, [refreshTick]);

    return [retVal, refresh];
}

export function useContainerNames(): [string[], () => void] {

    const [pods, refreshData] = usePods();
    const containers = React.useMemo(() => {
        return [...new Set(pods.map(pod => pod.ContainerName))];
    }, [pods]);

    return [containers, refreshData];
}

export function usePodNames(): [string[], () => void] {

    const [pods, refreshData] = usePods();
    const podNames = React.useMemo(() => {
        return pods.map(pod => pod.Name);
    }, [pods]);

    return [podNames, refreshData];
}