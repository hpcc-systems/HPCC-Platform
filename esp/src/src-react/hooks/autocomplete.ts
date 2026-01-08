import * as React from "react";
import { Query, WorkunitsService, DFUService, FileSprayService, AccessService } from "@hpcc-js/comms";
import { useBuildInfo } from "./platform";
import { useMyAccount } from "./user";

export interface SearchSuggestion {
    key: string;
    text: string;
    type: "page" | "prefix" | "recent" | "cluster" | "workunit" | "file" | "query" | "dfuworkunit" | "user";
    route?: string;
    requiresAdmin?: boolean;
}

// ECL Watch pages/routes
const ECLWATCH_PAGES: SearchSuggestion[] = [
    { key: "activities", text: "Activities", type: "page", route: "/activities" },
    { key: "workunits", text: "Workunits", type: "page", route: "/workunits" },
    { key: "dashboard", text: "Workunits Dashboard", type: "page", route: "/workunits/dashboard" },
    { key: "playground", text: "ECL Playground", type: "page", route: "/play" },
    { key: "files", text: "Logical Files", type: "page", route: "/files" },
    { key: "scopes", text: "File Scopes", type: "page", route: "/scopes" },
    { key: "landingzone", text: "Landing Zones", type: "page", route: "/landingzone" },
    { key: "dfuworkunits", text: "DFU Workunits", type: "page", route: "/dfuworkunits" },
    { key: "xref", text: "XRef", type: "page", route: "/xref" },
    { key: "queries", text: "Published Queries", type: "page", route: "/queries" },
    { key: "packagemaps", text: "Package Maps", type: "page", route: "/packagemaps" },
    { key: "topology", text: "Topology", type: "page", route: "/topology" },
    { key: "configuration", text: "Configuration", type: "page", route: "/topology/configuration" },
    { key: "pods", text: "Kubernetes Pods", type: "page", route: "/topology/pods" },
    { key: "services", text: "Services", type: "page", route: "/topology/services" },
    { key: "logs", text: "System Logs", type: "page", route: "/topology/logs" },
    { key: "wusummary", text: "Workunit Summary", type: "page", route: "/topology/wu-summary" },
    { key: "security", text: "Security", type: "page", route: "/topology/security", requiresAdmin: true },
    { key: "desdl", text: "Dynamic ESDL", type: "page", route: "/topology/desdl" },
    { key: "daliadmin", text: "Dali Admin", type: "page", route: "/topology/daliadmin" },
    { key: "sasha", text: "Sasha", type: "page", route: "/topology/sasha" },
    { key: "operations", text: "Operations", type: "page", route: "/operations" },
    { key: "diskusage", text: "Disk Usage", type: "page", route: "/operations/diskusage" },
    { key: "clusters", text: "Target Clusters", type: "page", route: "/operations/clusters" },
    { key: "processes", text: "Cluster Processes", type: "page", route: "/operations/processes" },
    { key: "servers", text: "System Servers", type: "page", route: "/operations/servers" },
    { key: "events", text: "Event Scheduler", type: "page", route: "/events" },
];

const SEARCH_PREFIXES: SearchSuggestion[] = [
    { key: "wuid:", text: "wuid:", type: "prefix" },
    { key: "dfu:", text: "dfu:", type: "prefix" },
    { key: "file:", text: "file:", type: "prefix" },
    { key: "query:", text: "query:", type: "prefix" },
    { key: "user:", text: "user:", type: "prefix", requiresAdmin: true },
    { key: "cluster:", text: "cluster:", type: "prefix" },
];

const workunitService = new WorkunitsService({ baseUrl: "" });
const dfuService = new DFUService({ baseUrl: "" }); // Used for file search
const fileSprayService = new FileSprayService({ baseUrl: "" }); // Used for DFU workunit search
const accessService = new AccessService({ baseUrl: "" }); // Used for user search

export const useSearchAutocomplete = () => {
    const [buildInfo] = useBuildInfo();
    const { isAdmin } = useMyAccount();
    const [suggestions, setSuggestions] = React.useState<SearchSuggestion[]>([]);
    const [isSearching, setIsSearching] = React.useState(false);

    const getRecentSearches = React.useCallback((): SearchSuggestion[] => {
        try {
            const recent = localStorage.getItem("eclwatch-recent-searches");
            if (recent) {
                const searches = JSON.parse(recent);
                // Validate that searches is an array and contains only strings
                if (!Array.isArray(searches)) {
                    console.warn("Invalid recent searches data: not an array");
                    localStorage.removeItem("eclwatch-recent-searches");
                    return [];
                }
                const validSearches = searches.filter(s => typeof s === "string" && s.trim().length > 0);
                if (validSearches.length !== searches.length) {
                    console.warn("Invalid recent searches data: contains non-string elements");
                    localStorage.setItem("eclwatch-recent-searches", JSON.stringify(validSearches));
                }
                return validSearches.slice(0, 5).map((search: string) => ({
                    key: `recent-${search}`,
                    text: search,
                    type: "recent" as const
                }));
            }
        } catch (e) {
            console.warn("Failed to load recent searches:", e);
        }
        return [];
    }, []);

    const saveRecentSearch = React.useCallback((searchTerm: string) => {
        if (!searchTerm.trim()) return;

        try {
            const recent = localStorage.getItem("eclwatch-recent-searches");
            let searches = recent ? JSON.parse(recent) : [];

            // Validate that searches is an array and contains only strings
            if (!Array.isArray(searches)) {
                console.warn("Invalid recent searches data: not an array, resetting");
                searches = [];
            } else {
                searches = searches.filter(s => typeof s === "string" && s.trim().length > 0);
            }

            searches = searches.filter((s: string) => s !== searchTerm);
            searches.unshift(searchTerm);
            searches = searches.slice(0, 10);

            localStorage.setItem("eclwatch-recent-searches", JSON.stringify(searches));
        } catch (e) {
            console.warn("Failed to save recent search:", e);
        }
    }, []);

    const getClusterSuggestions = React.useCallback((): SearchSuggestion[] => {
        if (buildInfo && buildInfo.TpTargetClusters) {
            return buildInfo.TpTargetClusters.slice(0, 3).map((cluster) => ({
                key: `cluster-${cluster.Name}`,
                text: cluster.Name,
                type: "cluster" as const,
                route: `/operations/clusters/${cluster.Name}`
            }));
        }
        return [];
    }, [buildInfo]);

    const searchWorkunits = React.useCallback(async (searchTerm: string): Promise<SearchSuggestion[]> => {
        try {
            const response = await workunitService.WUQuery({
                Wuid: searchTerm + "*",
                PageSize: 5,
                PageStartFrom: 0
            });

            if (response && response.Workunits && response.Workunits.ECLWorkunit) {
                return response.Workunits.ECLWorkunit.map((wu) => ({
                    key: `workunit-${wu.Wuid}`,
                    text: wu.Wuid,
                    type: "workunit" as const,
                    route: `/workunits/${wu.Wuid}`
                }));
            }
        } catch (e) {
            console.warn("Failed to search workunits:", e);
        }
        return [];
    }, []);

    const searchFiles = React.useCallback(async (searchTerm: string): Promise<SearchSuggestion[]> => {
        try {
            const response = await dfuService.DFUQuery({
                LogicalName: "*" + searchTerm + "*",
                PageSize: 5,
                PageStartFrom: 0
            });

            if (response && response.DFULogicalFiles && response.DFULogicalFiles.DFULogicalFile) {
                return response.DFULogicalFiles.DFULogicalFile.map((file) => ({
                    key: `file-${file.Name}`,
                    text: file.Name,
                    type: "file" as const,
                    route: `/files/${file.NodeGroup}/${encodeURIComponent(file.Name)}`
                }));
            }
        } catch (e) {
            console.warn("Failed to search files:", e);
        }
        return [];
    }, []);

    const searchQueries = React.useCallback(async (searchTerm: string): Promise<SearchSuggestion[]> => {
        try {
            const queries = await Query.query({ baseUrl: "" }, {
                QueryName: "*" + searchTerm + "*"
            });

            if (queries && queries.length > 0) {
                return queries.slice(0, 5).map((query) => ({
                    key: `query-${query.QuerySetId}-${query.Id}`,
                    text: query.QueryName || query.Id,
                    type: "query" as const,
                    route: `/queries/${query.QuerySetId}/${query.Id}`
                }));
            }
        } catch (e) {
            console.warn("Failed to search queries:", e);
        }
        return [];
    }, []);

    const searchDFUWorkunits = React.useCallback(async (searchTerm: string): Promise<SearchSuggestion[]> => {
        try {
            const response = await fileSprayService.GetDFUWorkunits({
                Wuid: searchTerm + "*",
                PageSize: 5,
                PageStartFrom: 0
            });

            if (response && response.results && response.results.DFUWorkunit) {
                return response.results.DFUWorkunit.map((dfu: any) => ({
                    key: `dfuworkunit-${dfu.ID}`,
                    text: dfu.ID,
                    type: "dfuworkunit" as const,
                    route: `/dfuworkunits/${dfu.ID}`
                }));
            }
        } catch (e) {
            console.warn("Failed to search DFU workunits:", e);
        }
        return [];
    }, []);

    const searchUsers = React.useCallback(async (searchTerm: string): Promise<SearchSuggestion[]> => {
        try {
            const response = await accessService.UserQuery({
                Name: "*" + searchTerm + "*",
                PageSize: 5,
                PageStartFrom: 0
            });

            if (response && response.Users && response.Users.User) {
                return response.Users.User.map((user: any) => ({
                    key: `user-${user.username}`,
                    text: user.username,
                    type: "user" as const,
                    route: `/security/users/${user.username}`,
                    requiresAdmin: true  // IMPORTANT: User search requires admin access
                }));
            }
        } catch (e) {
            console.warn("Failed to search users:", e);
        }
        return [];
    }, []);

    const detectSearchType = React.useCallback((searchTerm: string): { type: string; value: string } | null => {
        const term = searchTerm.toLowerCase().trim();

        if (term.startsWith("wuid:")) {
            return { type: "wuid", value: term.substring(5).trim() };
        } else if (term.startsWith("dfu:")) {
            return { type: "dfu", value: term.substring(4).trim() };
        } else if (term.startsWith("file:")) {
            return { type: "file", value: term.substring(5).trim() };
        } else if (term.startsWith("query:")) {
            return { type: "query", value: term.substring(6).trim() };
        } else if (term.startsWith("user:") && isAdmin) {
            return { type: "user", value: term.substring(5).trim() };
        } else if (term.startsWith("cluster:")) {
            return { type: "cluster", value: term.substring(8).trim() };
        } else if (/^W\d{8}-\d{6}/i.test(term)) {
            return { type: "wuid", value: term };
        } else if (/^D\d{8}-\d{6}/i.test(term)) {
            return { type: "dfu", value: term };
        }

        return null;
    }, [isAdmin]);

    const filterSuggestions = React.useCallback(async (searchTerm: string): Promise<SearchSuggestion[]> => {
        if (!searchTerm.trim()) {
            // Show all available suggestions when empty
            const filtered = ECLWATCH_PAGES.filter(page => !page.requiresAdmin || isAdmin);
            return [
                ...getRecentSearches(),
                ...filtered.slice(0, 10),
                ...getClusterSuggestions()
            ];
        }

        const term = searchTerm.toLowerCase();
        const searchType = detectSearchType(searchTerm);

        // If a specific search type is detected, fetch from API
        if (searchType && searchType.value.length >= 1) {
            setIsSearching(true);
            let entitySuggestions: SearchSuggestion[] = [];

            try {
                switch (searchType.type) {
                    case "wuid":
                        entitySuggestions = await searchWorkunits(searchType.value.toUpperCase());
                        break;
                    case "dfu":
                        entitySuggestions = await searchDFUWorkunits(searchType.value.toUpperCase());
                        break;
                    case "file":
                        entitySuggestions = await searchFiles(searchType.value);
                        break;
                    case "query":
                        entitySuggestions = await searchQueries(searchType.value);
                        break;
                    case "user":
                        entitySuggestions = await searchUsers(searchType.value);
                        break;
                    case "cluster":
                        const clusters = getClusterSuggestions().filter(c =>
                            c.text.toLowerCase().includes(searchType.value)
                        );
                        entitySuggestions = clusters;
                        break;
                }
            } finally {
                setIsSearching(false);
            }

            return entitySuggestions.slice(0, 10);
        }

        // Otherwise, filter pages and prefixes
        const filteredPages = ECLWATCH_PAGES.filter(page =>
            (!page.requiresAdmin || isAdmin) &&
            (page.text.toLowerCase().includes(term) || page.key.toLowerCase().includes(term))
        );

        const exactMatches = filteredPages.filter(page =>
            page.text.toLowerCase() === term || page.key.toLowerCase() === term
        );
        const startMatches = filteredPages.filter(page =>
            !exactMatches.includes(page) &&
            (page.text.toLowerCase().startsWith(term) || page.key.toLowerCase().startsWith(term))
        );
        const containsMatches = filteredPages.filter(page =>
            !exactMatches.includes(page) && !startMatches.includes(page)
        );

        const filtered: SearchSuggestion[] = [
            ...exactMatches,
            ...startMatches,
            ...containsMatches,
            ...getRecentSearches().filter(s => s.text.toLowerCase().includes(term)),
            ...getClusterSuggestions().filter(s => s.text.toLowerCase().includes(term))
        ];

        return filtered.slice(0, 10);
    }, [detectSearchType, getClusterSuggestions, getRecentSearches, isAdmin, searchDFUWorkunits, searchFiles, searchQueries, searchUsers, searchWorkunits]);

    React.useEffect(() => {
        const filtered = ECLWATCH_PAGES.filter(page => !page.requiresAdmin || isAdmin);
        const allSuggestions = [
            ...getRecentSearches(),
            ...filtered.slice(0, 10),
            ...getClusterSuggestions()
        ];
        setSuggestions(allSuggestions);
    }, [buildInfo, getRecentSearches, getClusterSuggestions, isAdmin]);

    return {
        suggestions,
        filterSuggestions,
        saveRecentSearch,
        isSearching
    };
};
