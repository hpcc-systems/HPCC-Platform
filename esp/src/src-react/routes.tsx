import * as React from "react";
import { Route, RouterContext } from "universal-router";
import { initialize, parsePage, parseSearch, parseSort, pushUrl, replaceUrl } from "./util/history";

export type MainNav = "activities" | "workunits" | "files" | "queries" | "topology" | "operations";

export interface RouteEx<R = any, C extends RouterContext = RouterContext> extends Route<R, C> {
    mainNav: MainNav[];
}

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
            { path: "/:Wuid(W\\d\\d\\d\\d\\d\\d\\d\\d-\\d\\d\\d\\d\\d\\d(?:-\\d+)?)", action: (ctx, params) => { replaceUrl(`/workunits/${params.Wuid}`); return false; } },
            { path: "/:Wuid(D\\d\\d\\d\\d\\d\\d\\d\\d-\\d\\d\\d\\d\\d\\d(?:-\\d+)?)", action: (ctx, params) => { replaceUrl(`/dfuworkunits/${params.Wuid}`); return false; } },
            { path: "/:searchText", action: (ctx, { searchText }) => import("./components/Search").then(_ => <_.Search searchText={searchText as string} />) }
        ]
    },
    //  ECL  ---
    {
        mainNav: ["workunits"],
        path: "/workunits",
        children: [
            {
                path: "", action: (ctx) => import("./components/Workunits").then(_ => {
                    return <_.Workunits filter={parseSearch(ctx.search) as any} sort={parseSort(ctx.search)} page={parsePage(ctx.search)} />;
                })
            },
            {
                path: "/dashboard", action: (ctx) => import("./components/WorkunitsDashboard").then(_ => {
                    return <_.WorkunitsDashboard filterProps={parseSearch(ctx.search) as any} />;
                })
            },
            {
                path: "/:Wuid", action: (ctx, params) => import("./components/WorkunitDetails").then(_ => {
                    return <_.WorkunitDetails wuid={params.Wuid as string} />;
                })
            },
            {
                path: "/:Wuid/:Tab", action: (ctx, params) => import("./components/WorkunitDetails").then(_ => {
                    return <_.WorkunitDetails wuid={params.Wuid as string} tab={params.Tab as string} queryParams={parseSearch(ctx.search) as any} />;
                })
            },
            {
                path: "/:Wuid/:Tab/:State", action: (ctx, params) => import("./components/WorkunitDetails").then(_ => {
                    return <_.WorkunitDetails wuid={params.Wuid as string} tab={params.Tab as string} state={(params.State as string)} queryParams={parseSearch(ctx.search) as any} />;
                })
            },
        ]
    },
    {
        mainNav: ["workunits"],
        path: ["/play", "/playground"],
        children: [
            {
                path: "", action: () => import("./components/ECLPlayground").then(_ => {
                    return <_.ECLPlayground />;
                })
            },
            {
                path: "/:Wuid", action: (ctx, params) => import("./components/ECLPlayground").then(_ => {
                    return <_.ECLPlayground wuid={params.Wuid as string} />;
                })
            },
        ]
    },
    //  Files  ---
    {
        mainNav: ["files"],
        path: "/files",
        children: [
            {
                path: "", action: (ctx) => import("./components/Files").then(_ => {
                    let filter = parseSearch(ctx.search);
                    if (Object.keys(filter).length === 0) {
                        filter = { LogicalFiles: true, SuperFiles: true, Indexes: true };
                    }
                    return <_.Files filter={filter as any} sort={parseSort(ctx.search)} page={parsePage(ctx.search)} />;
                })
            },
            {
                path: "/:Name", action: (ctx, params) => import("./components/FileDetails").then(_ => {
                    return <_.FileDetails cluster={undefined} logicalFile={params.Name as string} />;
                })
            },
            {
                path: "/:NodeGroup/:Name", action: (ctx, params) => import("./components/FileDetails").then(_ => {
                    return <_.FileDetails cluster={params.NodeGroup as string} logicalFile={params.Name as string} />;
                })
            },
            {
                path: "/:NodeGroup/:Name/:Tab", action: (ctx, params) => import("./components/FileDetails").then(_ => {
                    return <_.FileDetails cluster={params.NodeGroup as string} logicalFile={params.Name as string} tab={params.Tab as string} sort={parseSort(ctx.search)} queryParams={parseSearch(ctx.search) as any} />;
                })
            },
        ]
    },
    {
        mainNav: ["files"],
        path: "/scopes",
        children: [
            {
                path: "", action: (ctx) => import("./components/Scopes").then(_ => {
                    return <_.Scopes filter={parseSearch(ctx.search) as any} scope={"."} />;
                })
            },
            {
                path: "/:Scope", action: (ctx, params) => import("./components/Scopes").then(_ => {
                    return <_.Scopes filter={parseSearch(ctx.search) as any} scope={params.Scope as string} />;
                })
            },
        ]
    },
    {
        mainNav: ["files"],
        path: "/landingzone",
        children: [
            {
                path: "", action: (ctx) => import("./components/LandingZone").then(_ => {
                    return <_.LandingZone filter={parseSearch(ctx.search) as any} />;
                })
            },
            {
                path: "/preview/:logicalFile", action: (ctx, params) => import("./components/HexView").then(_ => {
                    return <_.HexView logicalFile={params.logicalFile as string} />;
                })
            },
        ],
    },
    {
        mainNav: ["files"],
        path: "/dfuworkunits",
        children: [
            {
                path: "", action: (ctx) => import("./components/DFUWorkunits").then(_ => {
                    return <_.DFUWorkunits filter={parseSearch(ctx.search) as any} />;
                })
            },
            {
                path: "/:Wuid", action: (ctx, params) => import("./components/DFUWorkunitDetails").then(_ => {
                    return <_.DFUWorkunitDetails wuid={params.Wuid as string} />;
                })
            },
            {
                path: "/:Wuid/:Tab", action: (ctx, params) => import("./components/DFUWorkunitDetails").then(_ => {
                    return <_.DFUWorkunitDetails wuid={params.Wuid as string} tab={params.Tab as string} />;
                })
            }
        ]
    },
    {
        mainNav: ["files"],
        path: "/xref",
        children: [
            { path: "", action: () => import("./components/Xrefs").then(_ => <_.Xrefs />) },
            {
                path: "/:Name", action: (ctx, params) => import("./components/XrefDetails").then(_ => {
                    return <_.XrefDetails name={params.Name as string} />;
                })
            },
            {
                path: "/:Name/:Tab", action: (ctx, params) => import("./components/XrefDetails").then(_ => {
                    return <_.XrefDetails name={params.Name as string} tab={params.Tab as string} />;
                })
            }
        ]
    },
    //  Roxie  ---
    {
        mainNav: ["queries"],
        path: "/queries",
        children: [
            {
                path: "", action: (ctx) => import("./components/Queries").then(_ => {
                    return <_.Queries filter={parseSearch(ctx.search) as any} sort={parseSort(ctx.search)} page={parsePage(ctx.search)} />;
                })
            },
            {
                path: "/:QuerySetId/:Id", action: (ctx, params) => import("./components/QueryDetails").then(_ => {
                    return <_.QueryDetails querySet={params.QuerySetId as string} queryId={params.Id as string} />;
                })
            },
            {
                path: "/:QuerySetId/:Id/:Tab", action: (ctx, params) => import("./components/QueryDetails").then(_ => {
                    return <_.QueryDetails querySet={params.QuerySetId as string} queryId={params.Id as string} tab={params.Tab as string} />;
                })
            },
            {
                path: "/:QuerySetId/:Id/metrics/:Wuid", action: (ctx, params) => import("./components/QueryDetails").then(_ => {
                    return <_.QueryDetails querySet={params.QuerySetId as string} queryId={params.Id as string} tab="metrics" metricsTab={params.Wuid as string} />;
                })
            },
            {
                path: "/:QuerySetId/:Id/metrics/:Wuid/:State", action: (ctx, params) => import("./components/QueryDetails").then(_ => {
                    return <_.QueryDetails querySet={params.QuerySetId as string} queryId={params.Id as string} tab="metrics" metricsTab={params.Wuid as string} state={params.State as string} />;
                })
            },
            {
                path: "/:QuerySetId/:QueryId/graphs/:Wuid/:GraphName", action: (ctx, params) => import("./layouts/DojoAdapter").then(_ => {
                    return <_.DojoAdapter widgetClassID="GraphTree7Widget" params={params} />;
                })
            },
            {
                path: "/:QuerySetId/:Id/testPages/:Tab", action: (ctx, params) => import("./components/QueryDetails").then(_ => {
                    return <_.QueryDetails querySet={params.QuerySetId as string} queryId={params.Id as string} tab="testPages" testTab={params.Tab as string} />;
                })
            },
        ]
    },
    {
        mainNav: ["queries"],
        path: "/packagemaps",
        children: [
            {
                path: "", action: (ctx, params) => import("./components/PackageMaps").then(_ => {
                    return <_.PackageMaps filter={parseSearch(ctx.search) as any} />;
                })
            },
            {
                path: "/validate/:Tab", action: (ctx, params) => import("./components/PackageMaps").then(_ => {
                    return <_.PackageMaps filter={parseSearch(ctx.search) as any} tab={params.Tab as string} />;
                })
            },
            {
                path: "/:Name", action: (ctx, params) => import("./components/PackageMapDetails").then(_ => {
                    return <_.PackageMapDetails name={params.Name as string} />;
                })
            },
            {
                path: "/:Name/:Tab", action: (ctx, params) => import("./components/PackageMapDetails").then(_ => {
                    return <_.PackageMapDetails name={params.Name as string} tab={params.Tab as string} />;
                })
            },
            {
                path: "/:Name/parts/:Part", action: (ctx, params) => import("./components/PackageMapPartDetails").then(_ => {
                    return <_.PackageMapPartDetails name={params.Name as string} part={params.Part as string} />;
                })
            },

        ]
    },
    //  Ops  ---
    {
        mainNav: ["topology"],
        path: "/topology",
        children: [
            {
                path: "", action: (ctx, params) => import("./components/Configuration").then(_ => {
                    return <_.Configuration />;
                })
            },
            {
                path: "/configuration", action: (ctx, params) => import("./components/Configuration").then(_ => {
                    return <_.Configuration />;
                })
            },
            {
                path: "/pods", action: (ctx, params) => import("./components/Pods").then(_ => {
                    return <_.Pods />;
                })
            },
            {
                path: "/pods-json", action: (ctx, params) => import("./components/Pods").then(_ => {
                    return <_.PodsJSON />;
                })
            },
            {
                path: "/services", action: (ctx, params) => import("./components/Services").then(_ => {
                    return <_.Services />;
                })
            },
            {
                path: "/logs", action: (ctx) => import("./components/Logs").then(_ => {
                    let filter = parseSearch(ctx.search) as any;
                    filter = Object.keys(filter).length < 1 ? _.defaultFilter : filter;
                    return <_.Logs filter={filter} />;
                })
            },
            {
                path: "/security",
                children: [
                    {
                        path: "", action: (ctx, params) => import("./components/Security").then(_ => {
                            return <_.Security filter={parseSearch(ctx.search) as any} page={parsePage(ctx.search)} />;
                        })
                    },
                    {
                        path: "/:Tab", action: (ctx, params) => import("./components/Security").then(_ => {
                            return <_.Security filter={parseSearch(ctx.search) as any} tab={params.Tab as string} page={parsePage(ctx.search)} />;
                        })
                    },
                    {
                        path: "/users/:username", action: (ctx, params) => import("./components/UserDetails").then(_ => {
                            return <_.UserDetails username={params.username as string} />;
                        })
                    },
                    {
                        path: "/users/:username/:Tab", action: (ctx, params) => import("./components/UserDetails").then(_ => {
                            return <_.UserDetails username={params.username as string} tab={params.Tab as string} />;
                        })
                    },
                    {
                        path: "/groups/:name", action: (ctx, params) => import("./components/GroupDetails").then(_ => {
                            return <_.GroupDetails name={params.name as string} />;
                        })
                    },
                    {
                        path: "/groups/:name/:Tab", action: (ctx, params) => import("./components/GroupDetails").then(_ => {
                            return <_.GroupDetails name={params.name as string} tab={params.Tab as string} />;
                        })
                    },
                    {
                        path: "/permissions/:Name/:BaseDn", action: (ctx, params) => import("./components/Security").then(_ => {
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
                            return <_.DynamicESDL />;
                        })
                    },
                    {
                        path: "/:Tab", action: (ctx, params) => import("./components/DynamicESDL").then(_ => {
                            return <_.DynamicESDL tab={params.Tab as string} />;
                        })
                    },
                    {
                        path: "/bindings/:Name", action: (ctx, params) => import("./components/DESDLBindingDetails").then(_ => {
                            return <_.DESDLBindingDetails name={params.Name as string} />;
                        })
                    },
                    {
                        path: "/bindings/:Name/:Tab", action: (ctx, params) => import("./components/DESDLBindingDetails").then(_ => {
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
                            return <_.DaliAdmin />;
                        })
                    },
                    {
                        path: "/:Tab", action: (ctx, params) => import("./components/DaliAdmin").then(_ => {
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
        children: [
            {
                path: "", action: () => import("./layouts/DojoAdapter").then(_ => {
                    return <_.DojoAdapter widgetClassID="TopologyWidget" />;
                })
            },
            {
                path: "/diskusage", action: () => import("./layouts/DojoAdapter").then(_ => {
                    return <_.DojoAdapter widgetClassID="DiskUsageWidget" />;
                })
            },
            {
                path: "/clusters",
                children: [
                    {
                        path: "", action: () => import("./layouts/DojoAdapter").then(_ => {
                            return <_.DojoAdapter widgetClassID="TargetClustersQueryWidget" />;
                        })
                    },
                    {
                        path: "/:ClusterName", action: (ctx, params) => import("./layouts/DojoAdapter").then(_ => {
                            return <_.DojoAdapter widgetClassID="TpClusterInfoWidget" params={params} />;
                        })
                    },
                    {
                        path: "/:Cluster/usage", action: (ctx, params) => import("./components/DiskUsage").then(_ => {
                            return <_.ClusterUsage cluster={params.Cluster as string} />;
                        })
                    },
                ]
            },
            {
                path: "/processes", action: () => import("./layouts/DojoAdapter").then(_ => {
                    return <_.DojoAdapter widgetClassID="ClusterProcessesQueryWidget" />;
                })
            },
            {
                path: "/servers", action: () => import("./layouts/DojoAdapter").then(_ => {
                    return <_.DojoAdapter widgetClassID="SystemServersQueryWidget" />;
                })
            },
            {
                path: "/:Machine/usage", action: (ctx, params) => import("./components/DiskUsage").then(_ => {
                    return <_.MachineUsage machine={params.Machine as string} />;
                })
            },
            {

                path: "/security",
                children: [
                    {
                        path: "", action: (ctx, params) => import("./components/Security").then(_ => {
                            return <_.Security filter={parseSearch(ctx.search) as any} page={parsePage(ctx.search)} />;
                        })
                    },
                    {
                        path: "/:Tab", action: (ctx, params) => import("./components/Security").then(_ => {
                            return <_.Security filter={parseSearch(ctx.search) as any} tab={params.Tab as string} page={parsePage(ctx.search)} />;
                        })
                    },
                    {
                        path: "/users/:username", action: (ctx, params) => import("./components/UserDetails").then(_ => {
                            return <_.UserDetails username={params.username as string} />;
                        })
                    },
                    {
                        path: "/users/:username/:Tab", action: (ctx, params) => import("./components/UserDetails").then(_ => {
                            return <_.UserDetails username={params.username as string} tab={params.Tab as string} />;
                        })
                    },
                    {
                        path: "/groups/:name", action: (ctx, params) => import("./components/GroupDetails").then(_ => {
                            return <_.GroupDetails name={params.name as string} />;
                        })
                    },
                    {
                        path: "/groups/:name/:Tab", action: (ctx, params) => import("./components/GroupDetails").then(_ => {
                            return <_.GroupDetails name={params.name as string} tab={params.Tab as string} />;
                        })
                    },
                    {
                        path: "/permissions/:Name/:BaseDn", action: (ctx, params) => import("./components/Security").then(_ => {
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
                            return <_.DynamicESDL />;
                        })
                    },
                    {
                        path: "/:Tab", action: (ctx, params) => import("./components/DynamicESDL").then(_ => {
                            return <_.DynamicESDL tab={params.Tab as string} />;
                        })
                    },
                    {
                        path: "/bindings/:Name", action: (ctx, params) => import("./components/DESDLBindingDetails").then(_ => {
                            return <_.DESDLBindingDetails name={params.Name as string} />;
                        })
                    },
                    {
                        path: "/bindings/:Name/:Tab", action: (ctx, params) => import("./components/DESDLBindingDetails").then(_ => {
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
