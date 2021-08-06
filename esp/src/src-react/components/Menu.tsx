import * as React from "react";
import { IconButton, IContextualMenuItem, INavLinkGroup, Nav, Pivot, PivotItem, Stack, useTheme } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import nlsHPCC from "src/nlsHPCC";
import { MainNav, routes } from "../routes";
import { pushUrl } from "../util/history";
import { Breadcrumbs } from "./Breadcrumbs";
import { useFavorites, useHistory } from "../hooks/favorite";

//  Top Level Nav  ---
const navLinkGroups: INavLinkGroup[] = [
    {
        links: [
            {
                name: nlsHPCC.Activities,
                url: "#/activities",
                icon: "Home",
                key: "activities"
            },
            {
                name: nlsHPCC.ECL,
                url: "#/workunits",
                icon: "SetAction",
                key: "workunits"
            },
            {
                name: nlsHPCC.Files,
                url: "#/files",
                icon: "PageData",
                key: "files"
            },
            {
                name: nlsHPCC.PublishedQueries,
                url: "#/queries",
                icon: "Globe",
                key: "queries"
            },
            {
                name: nlsHPCC.Operations,
                url: "#/topology",
                icon: "Admin",
                key: "topology"
            }
        ]
    }
];

const navIdx: { [id: string]: MainNav[] } = {};

function append(route, path) {
    if (!navIdx[path]) {
        navIdx[path] = [];
    }
    route.mainNav?.forEach(item => {
        navIdx[path].push(item);
    });
}

routes.forEach((route: any) => {
    if (Array.isArray(route.path)) {
        route.path.forEach(path => {
            append(route, path);
        });
    } else {
        append(route, route.path);
    }
});

function navSelectedKey(hashPath) {
    const rootPath = navIdx[`/${hashPath?.split("/")[1]}`];
    if (rootPath?.length) {
        return rootPath[0];
    }
    return null;
}

const FIXED_WIDTH = 38;

interface MainNavigationProps {
    hashPath: string;
    useDarkMode: boolean;
    setUseDarkMode: (_: boolean) => void;
}

export const MainNavigation: React.FunctionComponent<MainNavigationProps> = ({
    hashPath,
    useDarkMode,
    setUseDarkMode
}) => {

    const theme = useTheme();

    const menu = useConst([...navLinkGroups]);

    const selKey = React.useMemo(() => {
        return navSelectedKey(hashPath);
    }, [hashPath]);

    return <Stack verticalAlign="space-between" styles={{ root: { width: `${FIXED_WIDTH}px`, height: "100%", position: "relative", backgroundColor: theme.palette.themeLighterAlt } }}>
        <Stack.Item>
            <Nav selectedKey={selKey} groups={menu} />
        </Stack.Item>
        <Stack.Item>
            <IconButton iconProps={{ iconName: useDarkMode ? "Sunny" : "ClearNight" }} onClick={() => setUseDarkMode(!useDarkMode)} />
            <IconButton iconProps={{ iconName: "Settings" }} onClick={() => { }} />
        </Stack.Item>
    </Stack>;
};

//  Second Level Nav  ---
interface SubMenu {
    headerText: string;
    itemKey: string;
}

type SubMenuItems = { [nav: string]: SubMenu[] };

const subMenuItems: SubMenuItems = {
    "activities": [
        { headerText: nlsHPCC.Activities, itemKey: "/activities" },
        { headerText: nlsHPCC.Activities + " (L)", itemKey: "/activities/legacy" },
        { headerText: nlsHPCC.TargetClusters, itemKey: "/clusters" },
        { headerText: nlsHPCC.EventScheduler + " (L)", itemKey: "/events" }
    ],
    "workunits": [
        { headerText: nlsHPCC.Workunits, itemKey: "/workunits" },
        { headerText: nlsHPCC.Dashboard, itemKey: "/workunits/dashboard" },
        { headerText: nlsHPCC.Workunits + " (L)", itemKey: "/workunits/legacy" },
        { headerText: nlsHPCC.Playground, itemKey: "/play" },
        { headerText: nlsHPCC.Playground + " (L)", itemKey: "/play/legacy" },
    ],
    "files": [
        { headerText: nlsHPCC.LogicalFiles, itemKey: "/files" },
        { headerText: nlsHPCC.LogicalFiles + " (L)", itemKey: "/files/legacy" },
        { headerText: nlsHPCC.LandingZones, itemKey: "/landingzone" },
        { headerText: nlsHPCC.LandingZones + " (L)", itemKey: "/landingzone/legacy" },
        { headerText: nlsHPCC.Workunits, itemKey: "/dfuworkunits" },
        { headerText: nlsHPCC.Workunits + " (L)", itemKey: "/dfuworkunits/legacy" },
        { headerText: nlsHPCC.XRef, itemKey: "/xref" },
    ],
    "queries": [
        { headerText: nlsHPCC.Queries, itemKey: "/queries" },
        { headerText: nlsHPCC.Queries + " (L)", itemKey: "/queries/legacy" },
        { headerText: nlsHPCC.PackageMaps, itemKey: "/packagemaps" }
    ],
    "topology": [
        { headerText: nlsHPCC.Topology, itemKey: "/topology" },
        { headerText: nlsHPCC.DiskUsage, itemKey: "/diskusage" },
        { headerText: nlsHPCC.TargetClusters, itemKey: "/clusters2" },
        { headerText: nlsHPCC.ClusterProcesses, itemKey: "/processes" },
        { headerText: nlsHPCC.SystemServers, itemKey: "/servers" },
        { headerText: nlsHPCC.Security, itemKey: "/security" },
        { headerText: nlsHPCC.Monitoring, itemKey: "/monitoring" },
        { headerText: nlsHPCC.DESDL, itemKey: "/esdl" },
        { headerText: nlsHPCC.LogVisualization, itemKey: "/elk" },
    ],
};

const subNavIdx: { [id: string]: string[] } = {};

for (const key in subMenuItems) {
    const subNav = subMenuItems[key];
    subNav.forEach(item => {
        if (!subNavIdx[item.itemKey]) {
            subNavIdx[item.itemKey] = [];
        }
        subNavIdx[item.itemKey].push(key);
    });
}

function subNavSelectedKey(hashPath) {
    return !!subNavIdx[hashPath] ? hashPath : null;
}

const handleLinkClick = (item?: PivotItem) => {
    if (item?.props?.itemKey) {
        pushUrl(item.props.itemKey);
    }
};

interface SubNavigationProps {
    hashPath: string;
}

export const SubNavigation: React.FunctionComponent<SubNavigationProps> = ({
    hashPath,
}) => {

    const theme = useTheme();

    const [favorites] = useFavorites();
    const [history] = useHistory();

    const mainNav = React.useMemo(() => {
        return navSelectedKey(hashPath);
    }, [hashPath]);

    const subNav = React.useMemo(() => {
        return subNavSelectedKey(hashPath);
    }, [hashPath]);

    const altSubNav = React.useMemo(() => {
        const parts = hashPath.split("/");
        parts.shift();
        return parts.shift();
    }, [hashPath]);

    const favoriteMenu: IContextualMenuItem[] = React.useMemo(() => {
        const retVal: IContextualMenuItem[] = [];
        for (const key in favorites) {
            retVal.push({
                name: decodeURI(key),
                href: key,
                key,
            });
        }
        return retVal;
    }, [favorites]);

    return <div style={{ backgroundColor: theme.palette.themeLighter }}>
        <Stack horizontal horizontalAlign="space-between">
            <Stack.Item align="center" grow={1}>
                <Stack horizontal >
                    <Stack.Item grow={0} >
                        <Pivot selectedKey={subNav || altSubNav} onLinkClick={handleLinkClick} headersOnly={true} linkFormat="tabs" styles={{ root: { marginLeft: 4 }, text: { lineHeight: 20 }, link: { maxHeight: 20, marginRight: 4 }, linkContent: { maxHeight: 20 } }} >
                            {subMenuItems[mainNav]?.map(row => <PivotItem headerText={row.headerText} itemKey={row.itemKey} />)}
                        </Pivot>
                    </Stack.Item>
                    {!subNav &&
                        <Stack.Item grow={1}>
                            <Breadcrumbs hashPath={hashPath} ignoreN={1} />
                        </Stack.Item>
                    }
                </Stack>
            </Stack.Item>
            <Stack.Item align="center" grow={0}>
                <IconButton title={nlsHPCC.Advanced} iconProps={{ iconName: "History" }} menuProps={{ items: history }} />
                <IconButton title={nlsHPCC.Advanced} iconProps={{ iconName: favoriteMenu.length === 0 ? "FavoriteStar" : "FavoriteStarFill" }} menuProps={{ items: favoriteMenu }} />
            </Stack.Item>
        </Stack>
    </div>;
};
