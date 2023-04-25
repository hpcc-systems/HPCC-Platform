import * as React from "react";
import { Route, RouterContext } from "universal-router";
import { hashHistory, initialize, parsePage, parseSearch, parseSort, pushUrl, replaceUrl } from "./util/history";

export type MainNav = "activities" | "workunits" | "files" | "queries" | "topology" | "operations";

export interface RouteEx<R = any, C extends RouterContext = RouterContext> extends Route<R, C> {
    mainNav: MainNav[];
}

interface PreviousRoutePage {
    activities: string;
    workunits: string;
    files: string;
    queries: string;
    topology: string;
    operations: string;
}

const previousCategoryPage: PreviousRoutePage = {
    activities: "",
    workunits: "",
    files: "",
    queries: "",
    topology: "",
    operations: ""
};

const checkPreviousCategoryPage = (category: string, context: RouterContext) => {
    const previousPage = hashHistory.recent().length ? hashHistory.recent()[1] : null;
    const previousUrl = previousPage ? previousPage?.pathname + previousPage?.search : "";
    if (previousCategoryPage[category] !== "" && previousUrl.indexOf(previousCategoryPage[category]) < 0) {
        replaceUrl(previousCategoryPage[category], context.search);
        return context.next();
    } else {
        return context.next();
    }
};

type RoutesEx<R = any, C extends RouterContext = RouterContext> = Array<RouteEx<R, C>>;

export const routes: RoutesEx = [
    {
        mainNav: [],
        name: "login",
        path: "/login",
        action: (ctx) => import("./components/forms/Login").then(_ => <_.Login />)
    },
    //  Main  ---
    {
        mainNav: ["activities"],
        path: "",
        action: (context) => pushUrl("/activities")

    },
    {
        mainNav: ["activities"],
        path: "/activities",
        children: [
            { path: "", action: (context) => import("./components/Activities").then(_ => <_.Activities />) },
        ]
    },
    {
        mainNav: ["activities"],
        name: "events",
        path: "/events",
        children: [
            { path: "", action: (ctx) => import("./components/EventScheduler").then(_ => <_.EventScheduler filter={parseSearch(ctx.search) as any} sort={parseSort(ctx.search)} page={parsePage(ctx.search)} />) },
        ]
    },
    {
        mainNav: [],
        name: "search",
        path: "/search",
        children: [
            { path: "", action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="SearchResultsWidget" />) },
            { path: "/:Wuid(W\\d\\d\\d\\d\\d\\d\\d\\d-\\d\\d\\d\\d\\d\\d(?:-\\d+)?)", action: (ctx, params) => replaceUrl(`/workunits/${params.Wuid}`) },
            { path: "/:Wuid(D\\d\\d\\d\\d\\d\\d\\d\\d-\\d\\d\\d\\d\\d\\d(?:-\\d+)?)", action: (ctx, params) => replaceUrl(`/dfuworkunits/${params.Wuid}`) },
            { path: "/:searchText", action: (ctx, { searchText }) => import("./components/Search").then(_ => <_.Search searchText={searchText as string} />) }
        ]
    },
    //  ECL  ---
    {
        mainNav: ["workunits"],
        path: "/workunits",
        action: (context) => checkPreviousCategoryPage("workunits", context),
        children: [
            {
                path: "", action: (ctx) => import("./components/Workunits").then(_ => {
                    previousCategoryPage["workunits"] = "/workunits" + ctx.search;
                    return <_.Workunits filter={parseSearch(ctx.search) as any} sort={parseSort(ctx.search)} page={parsePage(ctx.search)} />;
                })
            },
            {
                path: "/dashboard", action: (ctx) => import("./components/WorkunitsDashboard").then(_ => {
                    previousCategoryPage["workunits"] = "/workunits/dashboard";
                    return <_.WorkunitsDashboard filterProps={parseSearch(ctx.search) as any} />;
                })
            },
            {
                path: "/:Wuid", action: (ctx, params) => import("./components/WorkunitDetails").then(_ => {
                    previousCategoryPage["workunits"] = `/workunits/${params.Wuid}`;
                    return <_.WorkunitDetails wuid={params.Wuid as string} />;
                })
            },
            {
                path: "/:Wuid/:Tab", action: (ctx, params) => import("./components/WorkunitDetails").then(_ => {
                    previousCategoryPage["workunits"] = `/workunits/${params.Wuid}/${params.Tab}`;
                    return <_.WorkunitDetails wuid={params.Wuid as string} tab={params.Tab as string} queryParams={parseSearch(ctx.search) as any} />;
                })
            },
            {
                path: "/:Wuid/:Tab/:State", action: (ctx, params) => import("./components/WorkunitDetails").then(_ => {
                    previousCategoryPage["workunits"] = `/workunits/${params.Wuid}/${params.Tab}/${params.State}`;
                    return <_.WorkunitDetails wuid={params.Wuid as string} tab={params.Tab as string} state={(params.State as string)} queryParams={parseSearch(ctx.search) as any} />;
                })
            },
        ]
    },
    {
        mainNav: ["workunits"],
        path: ["/play", "/playground"],
        action: (context) => checkPreviousCategoryPage("workunits", context),
        children: [
            {
                path: "", action: () => import("./components/ECLPlayground").then(_ => {
                    previousCategoryPage["workunits"] = "/play";
                    return <_.ECLPlayground />;
                })
            },
            {
                path: "/:Wuid", action: (ctx, params) => import("./components/ECLPlayground").then(_ => {
                    previousCategoryPage["workunits"] = `/play/${params.Wuid}`;
                    return <_.ECLPlayground wuid={params.Wuid as string} />;
                })
            },
        ]
    },
    //  Files  ---
    {
        mainNav: ["files"],
        path: "/files",
        action: (context) => checkPreviousCategoryPage("files", context),
        children: [
            {
                path: "", action: (ctx) => import("./components/Files").then(_ => {
                    let filter = parseSearch(ctx.search);
                    if (Object.keys(filter).length === 0) {
                        filter = { LogicalFiles: true, SuperFiles: true, Indexes: true };
                    }
                    previousCategoryPage["files"] = "/files" + ctx.search;
                    return <_.Files filter={filter as any} sort={parseSort(ctx.search)} page={parsePage(ctx.search)} />;
                })
            },
            {
                path: "/:Name", action: (ctx, params) => import("./components/FileDetails").then(_ => {
                    previousCategoryPage["files"] = `/files/${params.Name}`;
                    return <_.FileDetails cluster={undefined} logicalFile={params.Name as string} />;
                })
            },
            {
                path: "/:NodeGroup/:Name", action: (ctx, params) => import("./components/FileDetails").then(_ => {
                    previousCategoryPage["files"] = `/files/${params.NodeGroup}/${params.Name}`;
                    return <_.FileDetails cluster={params.NodeGroup as string} logicalFile={params.Name as string} />;
                })
            },
            {
                path: "/:NodeGroup/:Name/:Tab", action: (ctx, params) => import("./components/FileDetails").then(_ => {
                    previousCategoryPage["files"] = `/files/${params.NodeGroup}/${params.Name}/${params.Tab}`;
                    return <_.FileDetails cluster={params.NodeGroup as string} logicalFile={params.Name as string} tab={params.Tab as string} sort={parseSort(ctx.search)} queryParams={parseSearch(ctx.search) as any} />;
                })
            },
        ]
    },
    {
        mainNav: ["files"],
        path: "/scopes",
        action: (context) => checkPreviousCategoryPage("files", context),
        children: [
            {
                path: "", action: (ctx) => import("./components/Scopes").then(_ => {
                    previousCategoryPage["files"] = "/scopes";
                    return <_.Scopes filter={parseSearch(ctx.search) as any} scope={"."} />;
                })
            },
            {
                path: "/:Scope", action: (ctx, params) => import("./components/Scopes").then(_ => {
                    previousCategoryPage["files"] = `/scopes/${params.Scope}`;
                    return <_.Scopes filter={parseSearch(ctx.search) as any} scope={params.Scope as string} />;
                })
            },
        ]
    },
    {
        mainNav: ["files"],
        path: "/landingzone",
        action: (context) => checkPreviousCategoryPage("files", context),
        children: [
            {
                path: "", action: (ctx) => import("./components/LandingZone").then(_ => {
                    previousCategoryPage["files"] = "/landingzone";
                    return <_.LandingZone filter={parseSearch(ctx.search) as any} />;
                })
            },
            {
                path: "/preview/:logicalFile", action: (ctx, params) => import("./components/HexView").then(_ => {
                    previousCategoryPage["files"] = `/landingzone/preview/${params.logicalFile}`;
                    return <_.HexView logicalFile={params.logicalFile as string} />;
                })
            },
        ],
    },
    {
        mainNav: ["files"],
        path: "/dfuworkunits",
        action: (context) => checkPreviousCategoryPage("files", context),
        children: [
            {
                path: "", action: (ctx) => import("./components/DFUWorkunits").then(_ => {
                    previousCategoryPage["files"] = "/dfuworkunits" + ctx.search;
                    return <_.DFUWorkunits filter={parseSearch(ctx.search) as any} />;
                })
            },
            {
                path: "/:Wuid", action: (ctx, params) => import("./components/DFUWorkunitDetails").then(_ => {
                    previousCategoryPage["files"] = `/dfuworkunits/${params.Wuid}`;
                    return <_.DFUWorkunitDetails wuid={params.Wuid as string} />;
                })
            },
            {
                path: "/:Wuid/:Tab", action: (ctx, params) => import("./components/DFUWorkunitDetails").then(_ => {
                    previousCategoryPage["files"] = `/dfuworkunits/${params.Wuid}/${params.Tab}`;
                    return <_.DFUWorkunitDetails wuid={params.Wuid as string} tab={params.Tab as string} />;
                })
            }
        ]
    },
    {
        mainNav: ["files"],
        path: "/xref",
        action: (context) => checkPreviousCategoryPage("files", context),
        children: [
            { path: "", action: () => import("./components/Xrefs").then(_ => <_.Xrefs />) },
            {
                path: "/:Name", action: (ctx, params) => import("./components/XrefDetails").then(_ => {
                    previousCategoryPage["files"] = `/xref/${params.Name}`;
                    return <_.XrefDetails name={params.Name as string} />;
                })
            },
            {
                path: "/:Name/:Tab", action: (ctx, params) => import("./components/XrefDetails").then(_ => {
                    previousCategoryPage["files"] = `/xref/${params.Name}/${params.Tab}`;
                    return <_.XrefDetails name={params.Name as string} tab={params.Tab as string} />;
                })
            }
        ]
    },
    //  Roxie  ---
    {
        mainNav: ["queries"],
        path: "/queries",
        action: (context) => checkPreviousCategoryPage("queries", context),
        children: [
            {
                path: "", action: (ctx) => import("./components/Queries").then(_ => {
                    previousCategoryPage["queries"] = "/queries" + ctx.search;
                    return <_.Queries filter={parseSearch(ctx.search) as any} sort={parseSort(ctx.search)} page={parsePage(ctx.search)} />;
                })
            },
            {
                path: "/:QuerySetId/:Id", action: (ctx, params) => import("./components/QueryDetails").then(_ => {
                    previousCategoryPage["queries"] = `/queries/${params.QuerySetId}/${params.Id}`;
                    return <_.QueryDetails querySet={params.QuerySetId as string} queryId={params.Id as string} />;
                })
            },
            {
                path: "/:QuerySetId/:Id/:Tab", action: (ctx, params) => import("./components/QueryDetails").then(_ => {
                    previousCategoryPage["queries"] = `/queries/${params.QuerySetId}/${params.Id}/${params.Tab}`;
                    return <_.QueryDetails querySet={params.QuerySetId as string} queryId={params.Id as string} tab={params.Tab as string} />;
                })
            },
            {
                path: "/:QuerySetId/:QueryId/graphs/:Wuid/:GraphName", action: (ctx, params) => import("./layouts/DojoAdapter").then(_ => {
                    previousCategoryPage["queries"] = `/queries/${params.QuerySetId}/${params.QueryId}/graphs/${params.Wuid}/${params.GraphName}`;
                    return <_.DojoAdapter widgetClassID="GraphTree7Widget" params={params} />;
                })
            },
            {
                path: "/:QuerySetId/:Id/testPages/:Tab", action: (ctx, params) => import("./components/QueryDetails").then(_ => {
                    previousCategoryPage["queries"] = `/queries/${params.QuerySetId}/${params.Id}/testPages/${params.Tab}`;
                    return <_.QueryDetails querySet={params.QuerySetId as string} queryId={params.Id as string} tab="testPages" testTab={params.Tab as string} />;
                })
            },
        ]
    },
    {
        mainNav: ["queries"],
        path: "/packagemaps",
        action: (context) => checkPreviousCategoryPage("queries", context),
        children: [
            {
                path: "", action: (ctx, params) => import("./components/PackageMaps").then(_ => {
                    previousCategoryPage["queries"] = "/packagemaps" + ctx.search;
                    return <_.PackageMaps filter={parseSearch(ctx.search) as any} />;
                })
            },
            {
                path: "/validate/:Tab", action: (ctx, params) => import("./components/PackageMaps").then(_ => {
                    previousCategoryPage["queries"] = `/packagemaps/validate/${params.Tab}`;
                    return <_.PackageMaps filter={parseSearch(ctx.search) as any} tab={params.Tab as string} />;
                })
            },
            {
                path: "/:Name", action: (ctx, params) => import("./components/PackageMapDetails").then(_ => {
                    previousCategoryPage["queries"] = `/packagemaps/${params.Name}`;
                    return <_.PackageMapDetails name={params.Name as string} />;
                })
            },
            {
                path: "/:Name/:Tab", action: (ctx, params) => import("./components/PackageMapDetails").then(_ => {
                    previousCategoryPage["queries"] = `/packagemaps/${params.Name}/${params.Tab}`;
                    return <_.PackageMapDetails name={params.Name as string} tab={params.Tab as string} />;
                })
            },
            {
                path: "/:Name/parts/:Part", action: (ctx, params) => import("./components/PackageMapPartDetails").then(_ => {
                    previousCategoryPage["queries"] = `/packagemaps/${params.Name}/parts/${params.Part}`;
                    return <_.PackageMapPartDetails name={params.Name as string} part={params.Part as string} />;
                })
            },

        ]
    },
    //  Ops  ---
    {
        mainNav: ["topology"],
        path: "/topology",
        action: (context) => checkPreviousCategoryPage("topology", context),
        children: [
            {
                path: "", action: (ctx, params) => import("./components/Configuration").then(_ => {
                    previousCategoryPage["topology"] = "/topology";
                    return <_.Configuration />;
                })
            },
            {
                path: "/configuration", action: (ctx, params) => import("./components/Configuration").then(_ => {
                    previousCategoryPage["topology"] = "/topology/configuration";
                    return <_.Configuration />;
                })
            },
            {
                path: "/pods", action: (ctx, params) => import("./components/Pods").then(_ => {
                    previousCategoryPage["topology"] = "/topology/pods";
                    return <_.Pods />;
                })
            },
            {
                path: "/pods-json", action: (ctx, params) => import("./components/Pods").then(_ => {
                    previousCategoryPage["topology"] = "/topology/pods-json";
                    return <_.PodsJSON />;
                })
            },
            {
                path: "/services", action: (ctx, params) => import("./components/Services").then(_ => {
                    previousCategoryPage["topology"] = "/topology/services";
                    return <_.Services />;
                })
            },
            {
                path: "/logs", action: (ctx) => import("./components/Logs").then(_ => {
                    previousCategoryPage["topology"] = "/topology/logs";
                    return <_.Logs filter={parseSearch(ctx.search) as any} />;
                })
            },
            {
                path: "/security",
                children: [
                    {
                        path: "", action: (ctx, params) => import("./components/Security").then(_ => {
                            previousCategoryPage["topology"] = "/topology/security";
                            return <_.Security filter={parseSearch(ctx.search) as any} page={parsePage(ctx.search)} />;
                        })
                    },
                    {
                        path: "/:Tab", action: (ctx, params) => import("./components/Security").then(_ => {
                            previousCategoryPage["topology"] = `/topology/security/${params.Tab}`;
                            return <_.Security filter={parseSearch(ctx.search) as any} tab={params.Tab as string} page={parsePage(ctx.search)} />;
                        })
                    },
                    {
                        path: "/users/:username", action: (ctx, params) => import("./components/UserDetails").then(_ => {
                            previousCategoryPage["topology"] = `/topology/security/users/${params.username}`;
                            return <_.UserDetails username={params.username as string} />;
                        })
                    },
                    {
                        path: "/users/:username/:Tab", action: (ctx, params) => import("./components/UserDetails").then(_ => {
                            previousCategoryPage["topology"] = `/topology/security/users/${params.username}/${params.Tab}`;
                            return <_.UserDetails username={params.username as string} tab={params.Tab as string} />;
                        })
                    },
                    {
                        path: "/groups/:name", action: (ctx, params) => import("./components/GroupDetails").then(_ => {
                            previousCategoryPage["topology"] = `/topology/security/groups/${params.name}`;
                            return <_.GroupDetails name={params.name as string} />;
                        })
                    },
                    {
                        path: "/groups/:name/:Tab", action: (ctx, params) => import("./components/GroupDetails").then(_ => {
                            previousCategoryPage["topology"] = `/topology/security/groups/${params.name}/${params.Tab}`;
                            return <_.GroupDetails name={params.name as string} tab={params.Tab as string} />;
                        })
                    },
                    {
                        path: "/permissions/:Name/:BaseDn", action: (ctx, params) => import("./components/Security").then(_ => {
                            previousCategoryPage["topology"] = `/topology/security/permissions/${params.name}/${params.BaseDn}`;
                            return <_.Security tab="permissions" name={params.Name as string} baseDn={params.BaseDn as string} />;
                        })
                    },
                ]
            },
            {
                path: "/desdl",
                children: [
                    {
                        path: "", action: (ctx, params) => import("./components/DynamicESDL").then(_ => {
                            previousCategoryPage["topology"] = "/topology/desdl";
                            return <_.DynamicESDL />;
                        })
                    },
                    {
                        path: "/:Tab", action: (ctx, params) => import("./components/DynamicESDL").then(_ => {
                            previousCategoryPage["topology"] = `/topology/desdl/${params.Tab}`;
                            return <_.DynamicESDL tab={params.Tab as string} />;
                        })
                    },
                    {
                        path: "/bindings/:Name", action: (ctx, params) => import("./components/DESDLBindingDetails").then(_ => {
                            previousCategoryPage["topology"] = `/topology/desdl/bindings/${params.Name}`;
                            return <_.DESDLBindingDetails name={params.Name as string} />;
                        })
                    },
                    {
                        path: "/bindings/:Name/:Tab", action: (ctx, params) => import("./components/DESDLBindingDetails").then(_ => {
                            previousCategoryPage["topology"] = `/topology/desdl/bindings/${params.Name}/${params.Tab}`;
                            return <_.DESDLBindingDetails name={params.Name as string} tab={params.Tab as string} />;
                        })
                    },
                ]
            },
            {
                path: "/daliadmin",
                children: [
                    {
                        path: "", action: (ctx, params) => import("./components/DaliAdmin").then(_ => {
                            previousCategoryPage["topology"] = "/topology/daliadmin";
                            return <_.DaliAdmin />;
                        })
                    },
                    {
                        path: "/:Tab", action: (ctx, params) => import("./components/DaliAdmin").then(_ => {
                            previousCategoryPage["topology"] = `/topology/daliadmin/${params.Tab}`;
                            return <_.DaliAdmin tab={params.Tab as string} />;
                        })
                    },
                ]
            },
        ]
    },
    {
        mainNav: ["operations"],
        path: "/operations",
        action: (context) => checkPreviousCategoryPage("operations", context),
        children: [
            {
                path: "", action: () => import("./layouts/DojoAdapter").then(_ => {
                    previousCategoryPage["operations"] = "/operations";
                    return <_.DojoAdapter widgetClassID="TopologyWidget" />;
                })
            },
            {
                path: "/diskusage", action: () => import("./layouts/DojoAdapter").then(_ => {
                    previousCategoryPage["operations"] = "/operations/diskusage";
                    return <_.DojoAdapter widgetClassID="DiskUsageWidget" />;
                })
            },
            {
                path: "/clusters",
                children: [
                    {
                        path: "/clusters", action: () => import("./layouts/DojoAdapter").then(_ => {
                            previousCategoryPage["operations"] = "/operations/clusters";
                            return <_.DojoAdapter widgetClassID="TargetClustersQueryWidget" />;
                        })
                    },
                    {
                        path: "/:ClusterName", action: (ctx, params) => import("./layouts/DojoAdapter").then(_ => {
                            previousCategoryPage["operations"] = `/operations/${params.ClusterName}`;
                            return <_.DojoAdapter widgetClassID="TpClusterInfoWidget" params={params} />;
                        })
                    },
                    {
                        path: "/:Cluster/usage", action: (ctx, params) => import("./components/DiskUsage").then(_ => {
                            previousCategoryPage["operations"] = `/operations/${params.Cluster}/usage`;
                            return <_.ClusterUsage cluster={params.Cluster as string} />;
                        })
                    },
                ]
            },
            {
                path: "/processes", action: () => import("./layouts/DojoAdapter").then(_ => {
                    previousCategoryPage["operations"] = "/operations/processes";
                    return <_.DojoAdapter widgetClassID="ClusterProcessesQueryWidget" />;
                })
            },
            {
                path: "/servers", action: () => import("./layouts/DojoAdapter").then(_ => {
                    previousCategoryPage["operations"] = "/operations/servers";
                    return <_.DojoAdapter widgetClassID="SystemServersQueryWidget" />;
                })
            },
            {
                path: "/:Machine/usage", action: (ctx, params) => import("./components/DiskUsage").then(_ => {
                    previousCategoryPage["operations"] = `/operations/${params.Machine}/usage`;
                    return <_.MachineUsage machine={params.Machine as string} />;
                })
            },
            {

                path: "/security",
                children: [
                    {
                        path: "", action: (ctx, params) => import("./components/Security").then(_ => {
                            previousCategoryPage["operations"] = "/operations/security";
                            return <_.Security filter={parseSearch(ctx.search) as any} page={parsePage(ctx.search)} />;
                        })
                    },
                    {
                        path: "/:Tab", action: (ctx, params) => import("./components/Security").then(_ => {
                            previousCategoryPage["operations"] = `/operations/security/${params.Tab}`;
                            return <_.Security filter={parseSearch(ctx.search) as any} tab={params.Tab as string} page={parsePage(ctx.search)} />;
                        })
                    },
                    {
                        path: "/users/:username", action: (ctx, params) => import("./components/UserDetails").then(_ => {
                            previousCategoryPage["operations"] = `/operations/security/users/${params.username}`;
                            return <_.UserDetails username={params.username as string} />;
                        })
                    },
                    {
                        path: "/users/:username/:Tab", action: (ctx, params) => import("./components/UserDetails").then(_ => {
                            previousCategoryPage["operations"] = `/operations/security/users/${params.username}/${params.Tab}`;
                            return <_.UserDetails username={params.username as string} tab={params.Tab as string} />;
                        })
                    },
                    {
                        path: "/groups/:name", action: (ctx, params) => import("./components/GroupDetails").then(_ => {
                            previousCategoryPage["operations"] = `/operations/security/groups/${params.name}`;
                            return <_.GroupDetails name={params.name as string} />;
                        })
                    },
                    {
                        path: "/groups/:name/:Tab", action: (ctx, params) => import("./components/GroupDetails").then(_ => {
                            previousCategoryPage["operations"] = `/operations/security/groups/${params.name}/${params.Tab}`;
                            return <_.GroupDetails name={params.name as string} tab={params.Tab as string} />;
                        })
                    },
                    {
                        path: "/permissions/:Name/:BaseDn", action: (ctx, params) => import("./components/Security").then(_ => {
                            previousCategoryPage["operations"] = `/operations/security/permissions/${params.Name}/${params.BaseDn}`;
                            return <_.Security tab="permissions" name={params.Name as string} baseDn={params.BaseDn as string} />;
                        })
                    },
                ]
            },
            {
                path: "/desdl",
                children: [
                    {
                        path: "", action: (ctx, params) => import("./components/DynamicESDL").then(_ => {
                            previousCategoryPage["operations"] = "/operations/desdl";
                            return <_.DynamicESDL />;
                        })
                    },
                    {
                        path: "/:Tab", action: (ctx, params) => import("./components/DynamicESDL").then(_ => {
                            previousCategoryPage["operations"] = `/operations/desdl/${params.Tab}`;
                            return <_.DynamicESDL tab={params.Tab as string} />;
                        })
                    },
                    {
                        path: "/bindings/:Name", action: (ctx, params) => import("./components/DESDLBindingDetails").then(_ => {
                            previousCategoryPage["operations"] = `/operations/desdl/bindings/${params.Name}`;
                            return <_.DESDLBindingDetails name={params.Name as string} />;
                        })
                    },
                    {
                        path: "/bindings/:Name/:Tab", action: (ctx, params) => import("./components/DESDLBindingDetails").then(_ => {
                            previousCategoryPage["operations"] = `/operations/desdl/bindings/${params.Name}/${params.Tab}`;
                            return <_.DESDLBindingDetails name={params.Name as string} tab={params.Tab as string} />;
                        })
                    },
                ]
            }
        ]
    },
    {
        mainNav: [],
        path: "/errors", action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="InfoGridWidget" />)
    },
    {
        mainNav: [],
        path: "/log", action: () => import("./components/LogViewer").then(_ => <_.LogViewer />)
    },
    //  Other
    {
        mainNav: [],
        path: "/iframe", action: (ctx) => import("./components/IFrame").then(_ => <_.IFrame src={parseSearch(ctx.search).src as string} />)
    },
    {
        mainNav: [],
        path: "/text", action: (ctx) => {
            const params = parseSearch(ctx.search);
            return import("./components/SourceEditor").then(_ => <_.FetchEditor mode={params.mode as any} url={params.src as string} />);
        }
    },
    {
        mainNav: [],
        path: "(.*)", action: () => <h1>Not Found</h1>
    }
];

export const router = initialize(routes);
