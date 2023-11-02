import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { scopedLogger } from "@hpcc-js/util";
import { Topology, WsTopology } from "@hpcc-js/comms";
import { getBuildInfo, BuildInfo, fetchModernMode } from "src/Session";
import { cmake_build_type, containerized, ModernMode } from "src/BuildInfo";
import { sessionKeyValStore, userKeyValStore } from "src/KeyValStore";

const logger = scopedLogger("src-react/hooks/platform.ts");

declare const dojoConfig;

export function useBuildInfo(): [BuildInfo, { isContainer: boolean, currencyCode: string, opsCategory: string }] {

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

    const opsCategory = React.useMemo(() => {
        return cmake_build_type !== "Debug" ?
            // not a Debug build, check if containerized
            ((containerized) ? "topology" : "operations") :
            // Debug build, use first segment of current hash after #
            document.location.hash.indexOf("stub") > -1 ? "operations" : document.location.hash.split("/")[1];
    }, []);

    return [buildInfo, { isContainer, currencyCode, opsCategory }];
}

export function useLogicalClusters(): [WsTopology.TpLogicalCluster[] | undefined, WsTopology.TpLogicalCluster | undefined] {
    const [targetClusters, setTargetClusters] = React.useState<WsTopology.TpLogicalCluster[]>();
    const [defaultCluster, setDefaultCluster] = React.useState<WsTopology.TpLogicalCluster>();

    React.useEffect(() => {
        const topology = Topology.attach({ baseUrl: "" });
        let active = true;
        topology.fetchLogicalClusters().then(response => {
            if (active) {
                setTargetClusters(response);
                let firstRow: WsTopology.TpLogicalCluster;
                let firstHThor: WsTopology.TpLogicalCluster;
                let firstThor: WsTopology.TpLogicalCluster;
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

export function useModernMode(): {
    modernMode: string;
    setModernMode: (value: string) => void;
} {
    const userStore = useConst(() => userKeyValStore());
    const sessionStore = useConst(() => sessionKeyValStore());

    const [modernMode, setModernMode] = React.useState<string>("");

    React.useEffect(() => {
        fetchModernMode().then(mode => {
            setModernMode(String(mode));
        });
    }, [sessionStore, userStore]);

    React.useEffect(() => {
        if (modernMode === "") return;
        sessionStore.set(ModernMode, modernMode);
        userStore.set(ModernMode, modernMode);
    }, [modernMode, sessionStore, userStore]);

    return { modernMode, setModernMode };
}