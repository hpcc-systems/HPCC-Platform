import * as React from "react";
import { scopedLogger } from "@hpcc-js/util";
import { Topology, TpLogicalClusterQuery } from "@hpcc-js/comms";
import { getBuildInfo, BuildInfo } from "src/Session";

const logger = scopedLogger("src-react/hooks/platform.ts");

declare const dojoConfig;

export function useBuildInfo(): [BuildInfo, { isContainer: boolean, currencyCode: string }] {

    const [buildInfo, setBuildInfo] = React.useState<BuildInfo>({});
    const [isContainer, setIsContainer] = React.useState<boolean>(dojoConfig.isContainer);
    const [currencyCode, setCurrencyCode] = React.useState<string>(dojoConfig.currencyCode);

    React.useEffect(() => {
        getBuildInfo().then(info => {
            setIsContainer(info["CONTAINERIZED"] === "ON");
            setCurrencyCode(info["currencyCode"] ?? "");
            setBuildInfo(info);
        });
    }, []);

    return [buildInfo, { isContainer, currencyCode }];
}

export function useLogicalClusters(): [TpLogicalClusterQuery.TpLogicalCluster[] | undefined, TpLogicalClusterQuery.TpLogicalCluster | undefined] {
    const [targetClusters, setTargetClusters] = React.useState<TpLogicalClusterQuery.TpLogicalCluster[]>();
    const [defaultCluster, setDefaultCluster] = React.useState<TpLogicalClusterQuery.TpLogicalCluster>();

    React.useEffect(() => {
        const topology = Topology.attach({ baseUrl: "" });
        let active = true;
        topology.fetchLogicalClusters().then(response => {
            if (active) {
                setTargetClusters(response);
                let firstRow: TpLogicalClusterQuery.TpLogicalCluster;
                let firstHThor: TpLogicalClusterQuery.TpLogicalCluster;
                let firstThor: TpLogicalClusterQuery.TpLogicalCluster;
                response.forEach(row => {
                    if (firstRow === undefined) {
                        firstRow = row;
                    }
                    if (firstHThor === undefined && (row as any).type === "hthor") {
                        firstHThor = row;
                    }
                    if (firstThor === undefined && (row as any).type === "thor") {
                        firstThor = row;
                    }
                    return row;
                });
                setDefaultCluster(firstThor ?? firstHThor ?? firstRow);
            }
        }).catch(err => logger.error(err));
        return () => { active = false; };
    }, []);

    return [targetClusters, defaultCluster];
}
