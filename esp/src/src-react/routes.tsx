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
            { path: "/legacy", action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="ActivityWidget" />) }
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
            { path: "/legacy", action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="WUQueryWidget" />) },
            { path: "/:Wuid", action: (ctx, params) => import("./components/WorkunitDetails").then(_ => <_.WorkunitDetails wuid={params.Wuid as string} />) },
            { path: "/:Wuid/:Tab", action: (ctx, params) => import("./components/WorkunitDetails").then(_ => <_.WorkunitDetails wuid={params.Wuid as string} tab={params.Tab as string} />) },
            { path: "/:Wuid/outputs/:Name", action: (ctx, params) => import("./components/Result").then(_ => <_.Result wuid={params.Wuid as string} resultName={params.Name as string} filter={parseSearch(ctx.search) as any} />) },
        ]
    },
    {
        mainNav: ["workunits"],
        path: ["/play", "/playground"],
        children: [
            { path: "", action: () => import("./components/ECLPlayground").then(_ => <_.ECLPlayground />) },
            { path: "/legacy", action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="ECLPlaygroundWidget" />) },
            { path: "/:Wuid", action: (ctx, params) => import("./components/ECLPlayground").then(_ => <_.ECLPlayground wuid={params.Wuid as string} />) },
        ]
    },
    //  Files  ---
    {
        mainNav: ["files"],
        path: "/files",
        children: [
            { path: "", action: (context) => import("./components/Files").then(_ => <_.Files filter={parseSearch(context.search) as any} />) },
            { path: "/legacy", action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="DFUQueryWidget" />) },
            { path: "/:NodeGroup/:Name", action: (ctx, params) => import("./components/FileDetails").then(_ => <_.FileDetails cluster={params.NodeGroup as string} logicalFile={params.Name as string} />) },
            { path: "/:NodeGroup/:Name/:Tab", action: (ctx, params) => import("./components/FileDetails").then(_ => <_.FileDetails cluster={params.NodeGroup as string} logicalFile={params.Name as string} tab={params.Tab as string} />) },
        ]
    },
    {
        mainNav: ["files"],
        path: "/landingzone",
        children: [
            { path: "", action: (context) => import("./components/LandingZone").then(_ => <_.LandingZone filter={parseSearch(context.search) as any} />) },
            { path: "/legacy", action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="LZBrowseWidget" />) },
            { path: "/preview/:logicalFile", action: (ctx, params) => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="HexViewWidget" params={params} />) },
        ],
    },
    {
        mainNav: ["files"],
        path: "/dfuworkunits",
        children: [
            { path: "", action: (context) => import("./components/DFUWorkunits").then(_ => <_.DFUWorkunits filter={parseSearch(context.search) as any} />) },
            { path: "/legacy", action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="GetDFUWorkunitsWidget" />) },
            { path: "/:Wuid", action: (ctx, params) => import("./components/DFUWorkunitDetails").then(_ => <_.DFUWorkunitDetails wuid={params.Wuid as string} />) },
            { path: "/:Wuid/:Tab", action: (ctx, params) => import("./components/DFUWorkunitDetails").then(_ => <_.DFUWorkunitDetails wuid={params.Wuid as string} tab={params.Tab as string} />) }
        ]
    },
    {
        mainNav: ["files"],
        path: "/xref",
        children: [
            { path: "", action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="XrefQueryWidget" />) },
            { path: "/:XrefTarget", action: (ctx, params) => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="XrefDetailsWidget" params={params} />) }
        ]
    },
    //  Roxie  ---
    {
        mainNav: ["queries"],
        path: "/queries",
        children: [
            { path: "", action: (context) => import("./components/Queries").then(_ => <_.Queries filter={parseSearch(context.search) as any} />) },
            { path: "/legacy", action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="QuerySetQueryWidget" />) },
            { path: "/:QuerySetId/:Id", action: (ctx, params) => import("./components/QueryDetails").then(_ => <_.QueryDetails querySet={params.QuerySetId as string} queryId={params.Id as string} />) },
            { path: "/:QuerySetId/:Id/:Tab", action: (ctx, params) => import("./components/QueryDetails").then(_ => <_.QueryDetails querySet={params.QuerySetId as string} queryId={params.Id as string} tab={params.Tab as string} />) }
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
        path: "/clusters2", action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="TargetClustersQueryWidget" />)
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
            { path: "", action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="UserQueryWidget" />) },
            { path: "/:Users", action: (ctx, params) => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="UserDetailsWidget" params={params} />) }
        ]
    },
    {
        mainNav: ["topology"],
        path: "/monitoring", action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="MonitoringWidget" />)
    },
    {
        mainNav: ["topology"],
        path: "/esdl", action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="DynamicESDLQueryWidget" />)
    },
    {
        mainNav: ["topology"],
        path: "/elk", action: () => import("./layouts/DojoAdapter").then(_ => <_.DojoAdapter widgetClassID="IFrameWidget&src=http%3A%2F%2F10.240.61.210%3A5601%2Fapp%2Fkibana%23%2Fdashboard%2FMetricbeat-system-overview-ecs%3F_g%3D(refreshInterval%253A(pause%253A!t%252Cvalue%253A300000)%252Ctime%253A(from%253Anow%252Fd%252Cto%253Anow%252Fd))&__filter=isTrusted%3Dfalse%26screenX%3D0%26screenY%3D0%26clientX%3D0%26clientY%3D0%26ctrlKey%3Dfalse%26shiftKey%3Dfalse%26altKey%3Dfalse%26metaKey%3Dfalse%26button%3D0%26buttons%3D0%26pageX%3D0%26pageY%3D0%26x%3D0%26y%3D0%26offsetX%3D0%26offsetY%3D0%26movementX%3D0%26movementY%3D0%26toElement%3D%255Bobject%2520HTMLInputElement%255D%26layerX%3D-1280%26layerY%3D9994%26getModifierState%3Dfunction%2520getModifierState()%2520%257B%2520%255Bnative%2520code%255D%2520%257D%26initMouseEvent%3Dfunction%2520initMouseEvent()%2520%257B%2520%255Bnative%2520code%255D%2520%257D%26view%3D%255Bobject%2520Window%255D%26detail%3D0%26which%3D1%26initUIEvent%3Dfunction%2520initUIEvent()%2520%257B%2520%255Bnative%2520code%255D%2520%257D%26NONE%3D0%26CAPTURING_PHASE%3D1%26AT_TARGET%3D2%26BUBBLING_PHASE%3D3%26type%3Dclick%26target%3D%255Bobject%2520HTMLInputElement%255D%26currentTarget%3D%255Bobject%2520HTMLInputElement%255D%26eventPhase%3D2%26bubbles%3Dtrue%26cancelable%3Dtrue%26defaultPrevented%3Dfalse%26composed%3Dtrue%26timeStamp%3D265185.10500000045%26srcElement%3D%255Bobject%2520HTMLInputElement%255D%26returnValue%3Dtrue%26cancelBubble%3Dfalse%26path%3D%255Bobject%2520HTMLInputElement%255D%26path%3D%255Bobject%2520HTMLSpanElement%255D%26path%3D%255Bobject%2520HTMLDivElement%255D%26path%3D%255Bobject%2520HTMLDivElement%255D%26path%3D%255Bobject%2520HTMLDivElement%255D%26path%3D%255Bobject%2520HTMLBodyElement%255D%26path%3D%255Bobject%2520HTMLHtmlElement%255D%26path%3D%255Bobject%2520HTMLDocument%255D%26path%3D%255Bobject%2520Window%255D%26composedPath%3Dfunction%2520composedPath()%2520%257B%2520%255Bnative%2520code%255D%2520%257D%26stopPropagation%3Dfunction%2520stopPropagation()%2520%257B%2520%255Bnative%2520code%255D%2520%257D%26stopImmediatePropagation%3Dfunction%2520stopImmediatePropagation()%2520%257B%2520%255Bnative%2520code%255D%2520%257D%26preventDefault%3Dfunction%2520preventDefault()%2520%257B%2520%255Bnative%2520code%255D%2520%257D%26initEvent%3Dfunction%2520initEvent()%2520%257B%2520%255Bnative%2520code%255D%2520%257D" />)
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
