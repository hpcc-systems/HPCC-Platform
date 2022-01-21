import * as React from "react";
import { Route, RouterContext } from "universal-router";
import { initialize, parseSearch, pushUrl } from "./util/history";

export interface ToDoProps {
}

const ToDo: React.FunctionComponent<ToDoProps> = () => {
    return <h1>TODO</h1>;
};

export type MainNav = "activities" | "workunits" | "files" | "queries" | "topology";

export interface RouteEx<R = any, C extends RouterContext = RouterContext> extends Route<R, C> {
    mainNav: MainNav[];
}

type RoutesEx<R = any, C extends RouterContext = RouterContext> = Array<RouteEx<R, C>>;

export const routes: RoutesEx = [
    {
        mainNav: [],
        name: "login",
        path: "/login",
        children: [
            { path: "", action: (context) => <ToDo /> }
        ]
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
        mainNav: ["activities", "topology"],
        path: "/clusters",
        children: [
            { path: "/:ClusterName", action: (ctx, params) => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="TpClusterInfoWidget" params={params} />) },
            { path: "/:Cluster/usage", action: (ctx, params) => import("./components/DiskUsage").then(_ => <_.ClusterUsage cluster={params.Cluster as string} />) },
        ]
    },
    {
        mainNav: ["topology"],
        path: "/machines",
        children: [
            { path: "/:Machine/usage", action: (ctx, params) => import("./components/DiskUsage").then(_ => <_.MachineUsage machine={params.Machine as string} />) },
        ]
    },
    {
        mainNav: ["activities"],
        name: "events",
        path: "/events",
        children: [
            { path: "", action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="EventScheduleWorkunitWidget" />) },
            { path: "/:Event", action: (ctx, params) => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="WUDetailsWidget" params={params} />) }
        ]
    },
    {
        mainNav: [],
        name: "search",
        path: "/search",
        children: [
            { path: "", action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="SearchResultsWidget" />) },
            { path: "/:Wuid(W\\d\\d\\d\\d\\d\\d\\d\\d-\\d\\d\\d\\d\\d\\d(?:-\\d+)?)", action: (ctx, params) => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="WUDetailsWidget" params={params} />) },
            { path: "/:Wuid(D\\d\\d\\d\\d\\d\\d\\d\\d-\\d\\d\\d\\d\\d\\d(?:-\\d+)?)", action: (ctx, params) => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="DFUWUDetailsWidget" params={params} />) },
            { path: "/:searchText", action: (ctx, { searchText }) => import("./components/Search").then(_ => <_.Search searchText={searchText as string} />) }
        ]
    },
    //  ECL  ---
    {
        mainNav: ["workunits"],
        path: "/workunits",
        children: [
            { path: "", action: (ctx) => import("./components/Workunits").then(_ => <_.Workunits filter={parseSearch(ctx.search) as any} />) },
            { path: "/dashboard", action: (ctx) => import("./components/WorkunitsDashboard").then(_ => <_.WorkunitsDashboard filterProps={parseSearch(ctx.search) as any} />) },
            { path: "/:Wuid", action: (ctx, params) => import("./components/WorkunitDetails").then(_ => <_.WorkunitDetails wuid={params.Wuid as string} />) },
            { path: "/:Wuid/:Tab", action: (ctx, params) => import("./components/WorkunitDetails").then(_ => <_.WorkunitDetails wuid={params.Wuid as string} tab={params.Tab as string} />) },
            { path: "/:Wuid/:Tab/:State", action: (ctx, params) => import("./components/WorkunitDetails").then(_ => <_.WorkunitDetails wuid={params.Wuid as string} tab={params.Tab as string} state={(params.State as string)} queryParams={parseSearch(ctx.search) as any} />) },
        ]
    },
    {
        mainNav: ["workunits"],
        path: ["/play", "/playground"],
        children: [
            { path: "", action: () => import("./components/ECLPlayground").then(_ => <_.ECLPlayground />) },
            { path: "/:Wuid", action: (ctx, params) => import("./components/ECLPlayground").then(_ => <_.ECLPlayground wuid={params.Wuid as string} />) },
        ]
    },
    //  Files  ---
    {
        mainNav: ["files"],
        path: "/files",
        children: [
            { path: "", action: (context) => import("./components/Files").then(_ => <_.Files filter={parseSearch(context.search) as any} />) },
            { path: "/:Name", action: (ctx, params) => import("./components/FileDetails").then(_ => <_.FileDetails cluster={undefined} logicalFile={params.Name as string} />) },
            { path: "/:NodeGroup/:Name", action: (ctx, params) => import("./components/FileDetails").then(_ => <_.FileDetails cluster={params.NodeGroup as string} logicalFile={params.Name as string} />) },
            { path: "/:NodeGroup/:Name/:Tab", action: (ctx, params) => import("./components/FileDetails").then(_ => <_.FileDetails cluster={params.NodeGroup as string} logicalFile={params.Name as string} tab={params.Tab as string} />) },
        ]
    },
    {
        mainNav: ["files"],
        path: "/landingzone",
        children: [
            { path: "", action: (context) => import("./components/LandingZone").then(_ => <_.LandingZone filter={parseSearch(context.search) as any} />) },
            { path: "/preview/:logicalFile", action: (ctx, params) => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="HexViewWidget" params={params} />) },
        ],
    },
    {
        mainNav: ["files"],
        path: "/dfuworkunits",
        children: [
            { path: "", action: (context) => import("./components/DFUWorkunits").then(_ => <_.DFUWorkunits filter={parseSearch(context.search) as any} />) },
            { path: "/:Wuid", action: (ctx, params) => import("./components/DFUWorkunitDetails").then(_ => <_.DFUWorkunitDetails wuid={params.Wuid as string} />) },
            { path: "/:Wuid/:Tab", action: (ctx, params) => import("./components/DFUWorkunitDetails").then(_ => <_.DFUWorkunitDetails wuid={params.Wuid as string} tab={params.Tab as string} />) }
        ]
    },
    {
        mainNav: ["files"],
        path: "/xref",
        children: [
            { path: "", action: () => import("./components/Xrefs").then(_ => <_.Xrefs />) },
            { path: "/:Name", action: (ctx, params) => import("./components/XrefDetails").then(_ => <_.XrefDetails name={params.Name as string} />) },
            { path: "/:Name/:Tab", action: (ctx, params) => import("./components/XrefDetails").then(_ => <_.XrefDetails name={params.Name as string} tab={params.Tab as string} />) }
        ]
    },
    //  Roxie  ---
    {
        mainNav: ["queries"],
        path: "/queries",
        children: [
            { path: "", action: (context) => import("./components/Queries").then(_ => <_.Queries filter={parseSearch(context.search) as any} />) },
            { path: "/:QuerySetId/:Id", action: (ctx, params) => import("./components/QueryDetails").then(_ => <_.QueryDetails querySet={params.QuerySetId as string} queryId={params.Id as string} />) },
            { path: "/:QuerySetId/:Id/:Tab", action: (ctx, params) => import("./components/QueryDetails").then(_ => <_.QueryDetails querySet={params.QuerySetId as string} queryId={params.Id as string} tab={params.Tab as string} />) },
            { path: "/:QuerySetId/:QueryId/graphs/:Wuid/:GraphName", action: (ctx, params) => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="GraphTree7Widget" params={params} />) },
            { path: "/:QuerySetId/:Id/testPages/:Tab", action: (ctx, params) => import("./components/QueryDetails").then(_ => <_.QueryDetails querySet={params.QuerySetId as string} queryId={params.Id as string} tab="testPages" testTab={params.Tab as string} />) },
        ]
    },
    {
        mainNav: ["queries"],
        path: "/packagemaps",
        children: [
            { path: "", action: (ctx, params) => import("./components/PackageMaps").then(_ => <_.PackageMaps filter={parseSearch(ctx.search) as any} />) },
            { path: "/validate/:Tab", action: (ctx, params) => import("./components/PackageMaps").then(_ => <_.PackageMaps filter={parseSearch(ctx.search) as any} tab={params.Tab as string} />) },
            { path: "/:Name", action: (ctx, params) => import("./components/PackageMapDetails").then(_ => <_.PackageMapDetails name={params.Name as string} />) },
            { path: "/:Name/:Tab", action: (ctx, params) => import("./components/PackageMapDetails").then(_ => <_.PackageMapDetails name={params.Name as string} tab={params.Tab as string} />) },
            { path: "/:Name/parts/:Part", action: (ctx, params) => import("./components/PackageMapPartDetails").then(_ => <_.PackageMapPartDetails name={params.Name as string} part={params.Part as string} />) },

        ]
    },
    //  Ops  ---
    {
        mainNav: ["topology"],
        path: "/topology",
        action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="TopologyWidget" />)
    },
    {
        mainNav: ["topology"],
        path: "/diskusage", action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="DiskUsageWidget" />)
    },
    {
        mainNav: ["topology"],
        path: "/clusters", action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="TargetClustersQueryWidget" />)
    },
    {
        mainNav: ["topology"],
        path: "/processes", action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="ClusterProcessesQueryWidget" />)
    },
    {
        mainNav: ["topology"],
        path: "/servers", action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="SystemServersQueryWidget" />)
    },
    {
        mainNav: ["topology"],
        path: "/security",
        children: [
            { path: "", action: (ctx, params) => import("./components/Security").then(_ => <_.Security filter={parseSearch(ctx.search) as any} />) },
            { path: "/:Tab", action: (ctx, params) => import("./components/Security").then(_ => <_.Security filter={parseSearch(ctx.search) as any} tab={params.Tab as string} />) },
            { path: "/users/:username", action: (ctx, params) => import("./components/UserDetails").then(_ => <_.UserDetails username={params.username as string} />) },
            { path: "/users/:username/:Tab", action: (ctx, params) => import("./components/UserDetails").then(_ => <_.UserDetails username={params.username as string} tab={params.Tab as string} />) },
            { path: "/groups/:name", action: (ctx, params) => import("./components/GroupDetails").then(_ => <_.GroupDetails name={params.name as string} />) },
            { path: "/groups/:name/:Tab", action: (ctx, params) => import("./components/GroupDetails").then(_ => <_.GroupDetails name={params.name as string} tab={params.Tab as string} />) },
            { path: "/permissions/:Name/:BaseDn", action: (ctx, params) => import("./components/Security").then(_ => <_.Security tab="permissions" name={params.Name as string} baseDn={params.BaseDn as string} />) },
        ]
    },
    {
        mainNav: ["topology"],
        path: "/monitoring", action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="MonitoringWidget" />)
    },
    {
        mainNav: ["topology"],
        path: "/desdl",
        children: [
            { path: "", action: (ctx, params) => import("./components/DynamicESDL").then(_ => <_.DynamicESDL />) },
            { path: "/:Tab", action: (ctx, params) => import("./components/DynamicESDL").then(_ => <_.DynamicESDL tab={params.Tab as string} />) },
            { path: "/bindings/:Name", action: (ctx, params) => import("./components/DESDLBindingDetails").then(_ => <_.DESDLBindingDetails name={params.Name as string} />) },
            { path: "/bindings/:Name/:Tab", action: (ctx, params) => import("./components/DESDLBindingDetails").then(_ => <_.DESDLBindingDetails name={params.Name as string} tab={params.Tab as string} />) },
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
    {
        mainNav: [],
        path: "/config", action: () => import("./components/Configuration").then(_ => <_.Configuration />)
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
