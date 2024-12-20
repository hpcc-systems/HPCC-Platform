import * as React from "react";
import { Octokit } from "octokit";
import { useConst } from "@fluentui/react-hooks";
import { scopedLogger } from "@hpcc-js/util";
import { LogaccessService, Topology, WsLogaccess, WsTopology, WorkunitsServiceEx } from "@hpcc-js/comms";
import nlsHPCC from "src/nlsHPCC";
import { getBuildInfo, BuildInfo, fetchModernMode } from "src/Session";
import { cmake_build_type, containerized, ModernMode } from "src/BuildInfo";
import { sessionKeyValStore, userKeyValStore } from "src/KeyValStore";
import { Palette } from "@hpcc-js/common";

const logger = scopedLogger("src-react/hooks/platform.ts");

export const service = new LogaccessService({ baseUrl: "" });

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

let g_targetCluster: Promise<WsTopology.TpLogicalCluster[]>;
export function useLogicalClusters(): [WsTopology.TpLogicalCluster[] | undefined, WsTopology.TpLogicalCluster | undefined] {
    const [targetClusters, setTargetClusters] = React.useState<WsTopology.TpLogicalCluster[]>();
    const [defaultCluster, setDefaultCluster] = React.useState<WsTopology.TpLogicalCluster>();

    React.useEffect(() => {
        if (!g_targetCluster) {
            const topology = Topology.attach({ baseUrl: "" });
            g_targetCluster = topology.fetchLogicalClusters();
        }
        let active = true;
        g_targetCluster.then(response => {
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

export function useLogicalClustersPalette(): [WsTopology.TpLogicalCluster[] | undefined, WsTopology.TpLogicalCluster | undefined, Palette.OrdinalPaletteFunc] {
    const [targetClusters, defaultCluster] = useLogicalClusters();

    const palette = useConst(() => Palette.ordinal("workunits", ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"]));

    React.useEffect(() => {
        if (targetClusters) {
            targetClusters.forEach(cluster => {
                palette(cluster.Name);
            });
        }
    }, [palette, targetClusters]);

    return [targetClusters, defaultCluster, palette];
}

let wuCheckFeaturesPromise;
export const fetchCheckFeatures = () => {
    if (!wuCheckFeaturesPromise) {
        const wuService = new WorkunitsServiceEx({ baseUrl: "" });
        wuCheckFeaturesPromise = wuService.WUCheckFeatures({ IncludeFullVersion: true });
    }
    return wuCheckFeaturesPromise;
};
export interface Features {
    major: number;
    minor: number;
    point: number;
    version: string;
    maturity: string;
    timestamp: Date;
}

export function useCheckFeatures(): Features {

    const [major, setMajor] = React.useState<number>();
    const [minor, setMinor] = React.useState<number>();
    const [point, setPoint] = React.useState<number>();
    const [version, setVersion] = React.useState<string>("");
    const [maturity, setMaturity] = React.useState<string>("");
    const [timestamp, setTimestamp] = React.useState<Date>();

    React.useEffect(() => {
        fetchCheckFeatures().then((response: any) => {
            setMajor(response.BuildVersionMajor);
            setMinor(response.BuildVersionMinor);
            setPoint(response.BuildVersionPoint);
            setVersion(response.BuildVersion);
            setMaturity(response.BuildMaturity);
            setTimestamp(new Date(response?.BuildTagTimestamp ?? Date.now()));
        }).catch(err => logger.error(err));
    }, []);

    return {
        major,
        minor,
        point,
        version,
        maturity,
        timestamp
    };
}
interface OctokitRelease {
    id: number;
    draft: boolean;
    prerelease: boolean;
    tag_name: string;
    html_url: string;
}
const fetchReleases = (): Promise<{ data: OctokitRelease[] }> => {
    const octokit = new Octokit({});
    return octokit.request("GET /repos/{owner}/{repo}/releases", {
        owner: "hpcc-systems",
        repo: "HPCC-Platform",
        per_page: 40,
        headers: {
            "X-GitHub-Api-Version": "2022-11-28"
        }
    });
};

const _fetchLatestReleases = (): Promise<OctokitRelease[]> => {
    return fetchReleases().then(response => {
        const latest: { [releaseID: string]: OctokitRelease } = response.data
            .filter(release => !release.draft || !release.prerelease)
            .reduce((prev, curr: OctokitRelease) => {
                const versionParts = curr.tag_name.split(".");
                versionParts.length = 2;
                const partialVersion = versionParts.join(".");
                if (!prev[partialVersion]) {
                    prev[partialVersion] = curr;
                }
                return prev;
            }, {});
        return Object.values(latest);
    }).catch(err => {
        logger.error(err);
        return [];
    });
};
let releasesPromise: Promise<any> | undefined;
export const fetchLatestReleases = (): Promise<OctokitRelease[]> => {
    if (!releasesPromise) {
        releasesPromise = _fetchLatestReleases();
    }
    return releasesPromise;
};

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

interface LogAccessInfo {
    logsEnabled: boolean;
    logsManagerType: string;
    logsColumns: WsLogaccess.Column[];
    logsStatusMessage: string;
}

export function useLogAccessInfo(): LogAccessInfo {
    const [logsEnabled, setLogsEnabled] = React.useState(false);
    const [logsManagerType, setLogsManagerType] = React.useState("");
    const [logsColumns, setLogsColumns] = React.useState<WsLogaccess.Column[]>();
    const [logsStatusMessage, setLogsStatusMessage] = React.useState("");

    React.useEffect(() => {
        service.GetLogAccessInfo({}).then(response => {
            if (response.hasOwnProperty("Exceptions")) {
                setLogsStatusMessage(response["Exceptions"]?.Exception[0]?.Message ?? nlsHPCC.LogAccess_GenericException);
            } else {
                if (response.RemoteLogManagerType === null) {
                    setLogsStatusMessage(nlsHPCC.LogAccess_LoggingNotConfigured);
                } else {
                    setLogsEnabled(true);
                    setLogsManagerType(response.RemoteLogManagerType);
                    setLogsColumns(response?.Columns?.Column);
                }
            }
        });
    }, []);

    return { logsEnabled, logsManagerType, logsColumns, logsStatusMessage };
}