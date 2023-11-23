import * as React from "react";
import { Octokit } from "octokit";
import { scopedLogger } from "@hpcc-js/util";
import { Topology, TpLogicalClusterQuery, WorkunitsServiceEx } from "@hpcc-js/comms";
import { getBuildInfo, BuildInfo } from "src/Session";
import { cmake_build_type, containerized } from "src/BuildInfo";

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

const fetchReleases = () => {
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
type ReleasesPromise = ReturnType<typeof fetchReleases>;
type ReleasesResponse = Awaited<ReleasesPromise>;
type Releases = ReleasesResponse["data"];
type Release = Releases[number];

const _fetchLatestReleases = (): Promise<Releases> => {
    return fetchReleases().then(response => {
        const latest: { [id: string]: Release } = response.data
            .filter(release => !release.draft || !release.prerelease)
            .reduce((prev, curr: Release) => {
                const versionParts = curr.tag_name.split(".");
                versionParts.length = 2;
                const partialVersion = versionParts.join(".");
                if (!prev[partialVersion]) {
                    prev[partialVersion] = curr;
                }
                return prev;
            }, {});
        return Object.values(latest) as Releases;
    }).catch(err => {
        logger.error(err);
        return [] as Releases;
    });
};
let releasesPromise: Promise<Releases> | undefined;
export const fetchLatestReleases = (): Promise<Releases> => {
    if (!releasesPromise) {
        releasesPromise = _fetchLatestReleases();
    }
    return releasesPromise;
};
