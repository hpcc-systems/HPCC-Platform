import * as React from "react";
import { IconButton, IContextualMenuItem, INavLinkGroup, Nav, Pivot, PivotItem, Stack, useTheme } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import nlsHPCC from "src/nlsHPCC";
import { MainNav, routes } from "../routes";
import { pushUrl } from "../util/history";
import { useFavorite, useFavorites, useHistory } from "../hooks/favorite";
import { useUserTheme } from "../hooks/theme";
import { Breadcrumbs } from "./Breadcrumbs";

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
}

export const MainNavigation: React.FunctionComponent<MainNavigationProps> = ({
    hashPath
}) => {

    const menu = useConst([...navLinkGroups]);
    const [theme, setTheme, isDark] = useUserTheme();

    const selKey = React.useMemo(() => {
        return navSelectedKey(hashPath);
    }, [hashPath]);

    return <Stack verticalAlign="space-between" styles={{ root: { width: `${FIXED_WIDTH}px`, height: "100%", position: "relative", backgroundColor: theme.palette.themeLighterAlt } }}>
        <Stack.Item>
            <Nav selectedKey={selKey} groups={menu} />
        </Stack.Item>
        <Stack.Item>
            <IconButton iconProps={{ iconName: isDark ? "Sunny" : "ClearNight" }} onClick={() => setTheme(isDark ? "light" : "dark")} />
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
        { headerText: nlsHPCC.EventScheduler + " (L)", itemKey: "/events" }
    ],
    "workunits": [
        { headerText: nlsHPCC.Workunits, itemKey: "/workunits" },
        // TODO: Post Tech Preview { headerText: nlsHPCC.Dashboard, itemKey: "/workunits/dashboard" },
        { headerText: nlsHPCC.Playground, itemKey: "/play" },
    ],
    "files": [
        { headerText: nlsHPCC.LogicalFiles, itemKey: "/files" },
        { headerText: nlsHPCC.LandingZones, itemKey: "/landingzone" },
        { headerText: nlsHPCC.Workunits, itemKey: "/dfuworkunits" },
        { headerText: nlsHPCC.XRef + " (L)", itemKey: "/xref" },
    ],
    "queries": [
        { headerText: nlsHPCC.Queries, itemKey: "/queries" },
        { headerText: nlsHPCC.PackageMaps, itemKey: "/packagemaps" }
    ],
    "topology": [
        { headerText: nlsHPCC.Topology + " (L)", itemKey: "/topology" },
        { headerText: nlsHPCC.DiskUsage + " (L)", itemKey: "/diskusage" },
        { headerText: nlsHPCC.TargetClusters + " (L)", itemKey: "/clusters" },
        { headerText: nlsHPCC.ClusterProcesses + " (L)", itemKey: "/processes" },
        { headerText: nlsHPCC.SystemServers + " (L)", itemKey: "/servers" },
        { headerText: nlsHPCC.Security + " (L)", itemKey: "/security" },
        { headerText: nlsHPCC.Monitoring + " (L)", itemKey: "/monitoring" },
        { headerText: nlsHPCC.DESDL + " (L)", itemKey: "/desdl" },
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
    const [isFavorite, addFavorite, removeFavorite] = useFavorite(window.location.hash);
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
                            {subMenuItems[mainNav]?.map(row => <PivotItem headerText={row.headerText} itemKey={row.itemKey} key={row.itemKey} />)}
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
                <IconButton title={nlsHPCC.History} iconProps={{ iconName: "History" }} menuProps={{ items: history }} />
                <IconButton title={nlsHPCC.Favorites} iconProps={{ iconName: isFavorite ? "FavoriteStarFill" : "FavoriteStar" }} menuProps={{ items: favoriteMenu }} split onClick={() => {
                    if (isFavorite) {
                        removeFavorite();
                    } else {
                        addFavorite();
                    }
                }} styles={{ splitButtonMenuButton: { backgroundColor: theme.palette.themeLighter, border: "none" } }} />
            </Stack.Item>
        </Stack>
    </div>;
};
