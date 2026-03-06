import * as React from "react";
import { WorkunitsService, DFUService, FileSprayService, AccessService } from "@hpcc-js/comms";
import * as WsWorkunits from "src/WsWorkunits";
import nlsHPCC from "src/nlsHPCC";
import { useLogicalClusters } from "./platform";
import { useMyAccount } from "./user";

export interface SearchSuggestion {
    key: string;
    text: string;
    type: "page" | "prefix" | "recent" | "cluster" | "workunit" | "file" | "query" | "dfuworkunit" | "user";
    route?: string;
    requiresAdmin?: boolean;
}

// ECL Watch pages/routes - using nlsHPCC for localization
const ECLWATCH_PAGES: SearchSuggestion[] = [
    { key: "activities", text: nlsHPCC.Activities, type: "page", route: "/activities" },
    { key: "workunits", text: nlsHPCC.Workunits, type: "page", route: "/workunits" },
    { key: "dashboard", text: nlsHPCC.Dashboard, type: "page", route: "/workunits/dashboard" },
    { key: "playground", text: nlsHPCC.title_ECLPlayground, type: "page", route: "/play" },
    { key: "files", text: nlsHPCC.LogicalFiles, type: "page", route: "/files" },
    { key: "scopes", text: nlsHPCC.FileScopes, type: "page", route: "/scopes" },
    { key: "landingzone", text: nlsHPCC.LandingZones, type: "page", route: "/landingzone" },
    { key: "dfuworkunits", text: nlsHPCC.title_GetDFUWorkunits, type: "page", route: "/dfuworkunits" },
    { key: "xref", text: nlsHPCC.XRef, type: "page", route: "/xref" },
    { key: "queries", text: nlsHPCC.PublishedQueries, type: "page", route: "/queries" },
    { key: "packagemaps", text: nlsHPCC.PackageMaps, type: "page", route: "/packagemaps" },
    { key: "topology", text: nlsHPCC.Topology, type: "page", route: "/topology" },
    { key: "configuration", text: nlsHPCC.Configuration, type: "page", route: "/topology/configuration" },
    { key: "pods", text: nlsHPCC.KubernetesPods, type: "page", route: "/topology/pods" },
    { key: "services", text: nlsHPCC.Services, type: "page", route: "/topology/services" },
    { key: "logs", text: nlsHPCC.SystemLogs, type: "page", route: "/topology/logs" },
    { key: "wusummary", text: nlsHPCC.WorkunitSummary, type: "page", route: "/topology/wu-summary" },
    { key: "security", text: nlsHPCC.Security, type: "page", route: "/topology/security", requiresAdmin: true },
    { key: "desdl", text: nlsHPCC.DynamicESDL, type: "page", route: "/topology/desdl" },
    { key: "daliadmin", text: nlsHPCC.DaliAdmin, type: "page", route: "/topology/daliadmin" },
    { key: "sasha", text: nlsHPCC.Sasha, type: "page", route: "/topology/sasha" },
    { key: "operations", text: nlsHPCC.Operations, type: "page", route: "/operations" },
    { key: "diskusage", text: nlsHPCC.DiskUsage, type: "page", route: "/operations/diskusage" },
    { key: "clusters", text: nlsHPCC.TargetClusters, type: "page", route: "/operations/clusters" },
    { key: "processes", text: nlsHPCC.ClusterProcesses, type: "page", route: "/operations/processes" },
    { key: "servers", text: nlsHPCC.SystemServers, type: "page", route: "/operations/servers" },
    { key: "events", text: nlsHPCC.EventScheduler, type: "page", route: "/events" },
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
    const [targetClusters] = useLogicalClusters();
    const { isAdmin } = useMyAccount();
    const [suggestions, setSuggestions] = React.useState<SearchSuggestion[]>([]);
    const [isSearching, setIsSearching] = React.useState(false);
    const abortControllerRef = React.useRef<AbortController | null>(null);

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
        if (targetClusters && targetClusters.length > 0) {
            return targetClusters.slice(0, 3).map((cluster) => ({
                key: `cluster-${cluster.Name}`,
                text: cluster.Name,
                type: "cluster" as const,
                route: `/operations/clusters/${encodeURIComponent(cluster.Name)}`
            }));
        }
        return [];
    }, [targetClusters]);

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
                    route: `/files/${encodeURIComponent(file.NodeGroup)}/${encodeURIComponent(file.Name)}`
                }));
            }
        } catch (e) {
            console.warn("Failed to search files:", e);
        }
        return [];
    }, []);

    const searchQueries = React.useCallback(async (searchTerm: string): Promise<SearchSuggestion[]> => {
        try {
            const response = await WsWorkunits.WUListQueries({
                request: {
                    QueryName: "*" + searchTerm + "*"
                }
            });

            if (response?.WUListQueriesResponse?.QuerysetQueries?.QuerySetQuery) {
                return response.WUListQueriesResponse.QuerysetQueries.QuerySetQuery.slice(0, 5).map((query: any) => ({
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
                    route: `/security/users/${encodeURIComponent(user.username)}`,
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
        // Cancel any pending requests
        // Note: AbortController checks if requests were aborted to prevent stale results from updating UI,
        // but the underlying @hpcc-js/comms library doesn't support passing abort signals to API calls,
        // so network requests won't be cancelled at the HTTP level.
        if (abortControllerRef.current) {
            abortControllerRef.current.abort();
        }

        if (!searchTerm.trim()) {
            setIsSearching(false);
            // Show all available suggestions when empty
            const filtered = ECLWATCH_PAGES.filter(page => !page.requiresAdmin || isAdmin);
            const prefixes = SEARCH_PREFIXES.filter(prefix => !prefix.requiresAdmin || isAdmin);
            return [
                ...prefixes,
                ...getRecentSearches(),
                ...filtered.slice(0, 10),
                ...getClusterSuggestions()
            ];
        }

        const term = searchTerm.toLowerCase();
        const searchType = detectSearchType(searchTerm);

        // If a specific search type is detected, fetch from API (only if at least 2 characters after prefix)
        if (searchType && searchType.value.length >= 2) {
            // Create new abort controller for this request
            abortControllerRef.current = new AbortController();
            const signal = abortControllerRef.current.signal;

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

                // Check if request was aborted
                if (signal.aborted) {
                    return [];
                }
            } catch (error) {
                // Ignore abort errors
                if (error.name !== "AbortError") {
                    console.warn("Search error:", error);
                }
                return [];
            } finally {
                if (!signal.aborted) {
                    setIsSearching(false);
                }
            }

            return entitySuggestions.slice(0, 10);
        }

        // No prefix detected - search across all entity types if search term is at least 2 characters
        if (searchTerm.length >= 2) {
            abortControllerRef.current = new AbortController();
            const signal = abortControllerRef.current.signal;

            setIsSearching(true);

            try {
                // Search all entity types in parallel
                const [workunits, files, queries, dfuWorkunits, users] = await Promise.all([
                    searchWorkunits(searchTerm.toUpperCase()).catch(() => []),
                    searchFiles(searchTerm).catch(() => []),
                    searchQueries(searchTerm).catch(() => []),
                    searchDFUWorkunits(searchTerm.toUpperCase()).catch(() => []),
                    isAdmin ? searchUsers(searchTerm).catch(() => []) : Promise.resolve([])
                ]);

                if (signal.aborted) {
                    return [];
                }

                // Combine results with type grouping - show max 3 per category
                const grouped: SearchSuggestion[] = [
                    ...workunits.slice(0, 3),
                    ...files.slice(0, 3),
                    ...queries.slice(0, 3),
                    ...dfuWorkunits.slice(0, 3),
                    ...users.slice(0, 3)
                ];

                // Also include matching pages and clusters
                const filteredPages = ECLWATCH_PAGES.filter(page =>
                    (!page.requiresAdmin || isAdmin) &&
                    (page.text.toLowerCase().includes(term) || page.key.toLowerCase().includes(term))
                ).slice(0, 3);

                const matchingClusters = getClusterSuggestions().filter(s =>
                    s.text.toLowerCase().includes(term)
                ).slice(0, 2);

                const filtered: SearchSuggestion[] = [
                    ...grouped,
                    ...filteredPages,
                    ...matchingClusters
                ];

                setIsSearching(false);
                return filtered.slice(0, 15);
            } catch (error) {
                if (error.name !== "AbortError") {
                    console.warn("Multi-search error:", error);
                }
                setIsSearching(false);
                return [];
            }
        }

        // For short search terms (1 character), just filter pages and prefixes
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
            ...SEARCH_PREFIXES.filter(s =>
                s.text.toLowerCase().includes(term) &&
                (!s.requiresAdmin || isAdmin)
            ),
            ...getRecentSearches().filter(s => s.text.toLowerCase().includes(term)),
            ...getClusterSuggestions().filter(s => s.text.toLowerCase().includes(term))
        ];

        return filtered.slice(0, 10);
    }, [detectSearchType, getClusterSuggestions, getRecentSearches, isAdmin, searchDFUWorkunits, searchFiles, searchQueries, searchUsers, searchWorkunits]);

    React.useEffect(() => {
        const filtered = ECLWATCH_PAGES.filter(page => !page.requiresAdmin || isAdmin);
        const prefixes = SEARCH_PREFIXES.filter(prefix => !prefix.requiresAdmin || isAdmin);
        const allSuggestions = [
            ...prefixes,
            ...getRecentSearches(),
            ...filtered.slice(0, 10),
            ...getClusterSuggestions()
        ];
        setSuggestions(allSuggestions);
    }, [targetClusters, getRecentSearches, getClusterSuggestions, isAdmin]);

    // Cleanup on unmount
    React.useEffect(() => {
        return () => {
            if (abortControllerRef.current) {
                abortControllerRef.current.abort();
            }
        };
    }, []);

    return {
        suggestions,
        filterSuggestions,
        saveRecentSearch,
        isSearching
    };
};
