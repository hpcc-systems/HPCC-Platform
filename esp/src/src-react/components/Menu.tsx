import * as React from "react";
import { IconButton, IContextualMenuItem, INavLink, INavLinkGroup, Link, mergeStyleSets, Nav, Stack } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import nlsHPCC from "src/nlsHPCC";
import { hasLogAccess } from "src/ESPLog";
import { containerized, bare_metal } from "src/BuildInfo";
import { MainNav, routes } from "../routes";
import { useFavorite, useFavorites, useHistory } from "../hooks/favorite";
import { useUserTheme } from "../hooks/theme";
import { usePivotItemDisable } from "../layouts/pivot";
import { Breadcrumbs } from "./Breadcrumbs";

//  Top Level Nav  ---
function navLinkGroups(): INavLinkGroup[] {
    let links: INavLink[] = [
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
            name: nlsHPCC.Topology,
            url: "#/topology",
            icon: "Org",
            key: "topology"
        },
        {
            name: nlsHPCC.Operations,
            url: "#/operations",
            icon: "Admin",
            key: "operations"
        }
    ];
    if (!containerized) {
        links = links.filter(l => l.key !== "topology");
    }
    if (!bare_metal) {
        links = links.filter(l => l.key !== "operations");
    }
    return [{ links }];
}

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

    const menu = useConst([...navLinkGroups()]);
    const { theme, setTheme, isDark } = useUserTheme();

    const selKey = React.useMemo(() => {
        return navSelectedKey(hashPath);
    }, [hashPath]);

    return <Stack verticalAlign="space-between" styles={{ root: { width: `${FIXED_WIDTH}px`, height: "100%", position: "relative", backgroundColor: theme.palette.themeLighterAlt } }}>
        <Stack.Item>
            <Nav selectedKey={selKey} groups={menu} />
        </Stack.Item>
        <Stack.Item>
            <IconButton
                iconProps={{ iconName: isDark ? "Sunny" : "ClearNight" }}
                onClick={() => {
                    setTheme(isDark ? "light" : "dark");
                    const themeChangeEvent = new CustomEvent("eclwatch-theme-toggle", {
                        detail: { dark: !isDark }
                    });
                    document.dispatchEvent(themeChangeEvent);
                }}
            />
            {/* Disable Theme editor button for launch of 9.0 */}
            {/* <IconButton iconProps={{ iconName: "Equalizer" }} onClick={() => { }} /> */}
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
        { headerText: nlsHPCC.EventScheduler, itemKey: "/events" }
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
        { headerText: nlsHPCC.Configuration, itemKey: "/topology/configuration" },
        { headerText: nlsHPCC.Pods, itemKey: "/topology/pods" },
        { headerText: nlsHPCC.Services, itemKey: "/topology/services" },
        { headerText: nlsHPCC.Logs, itemKey: "/topology/logs" },
        { headerText: nlsHPCC.Security + " (L)", itemKey: "/topology/security" },
        { headerText: nlsHPCC.DESDL + " (L)", itemKey: "/topology/desdl" },
        { headerText: nlsHPCC.DaliAdmin, itemKey: "/topology/daliadmin" },
    ],
    "operations": [
        { headerText: nlsHPCC.Topology + " (L)", itemKey: "/operations" },
        { headerText: nlsHPCC.DiskUsage + " (L)", itemKey: "/operations/diskusage" },
        { headerText: nlsHPCC.TargetClusters + " (L)", itemKey: "/operations/clusters" },
        { headerText: nlsHPCC.ClusterProcesses + " (L)", itemKey: "/operations/processes" },
        { headerText: nlsHPCC.SystemServers + " (L)", itemKey: "/operations/servers" },
        { headerText: nlsHPCC.Security + " (L)", itemKey: "/operations/security" },
        { headerText: nlsHPCC.DESDL + " (L)", itemKey: "/operations/desdl" },
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
    const hashCategory = hashPath.split("/").slice(0, 3).join("/");
    return !!subNavIdx[hashCategory] ? hashCategory : null;
}

interface SubNavigationProps {
    hashPath: string;
}

export const SubNavigation: React.FunctionComponent<SubNavigationProps> = ({
    hashPath,
}) => {

    const { theme } = useUserTheme();

    const [favorites] = useFavorites();
    const [favoriteCount, setFavoriteCount] = React.useState(0);
    const [isFavorite, addFavorite, removeFavorite] = useFavorite(window.location.hash);
    const [history] = useHistory();

    React.useEffect(() => {
        setFavoriteCount(Object.keys(favorites).length);
    }, [favorites]);

    const mainNav = React.useMemo(() => {
        return navSelectedKey(hashPath);
    }, [hashPath]);

    const subNav = React.useMemo(() => {
        return subNavSelectedKey(hashPath);
    }, [hashPath]);

    const altSubNav = React.useMemo(() => {
        return `/${hashPath?.split("/")[1]}`;
    }, [hashPath]);

    const navStyles = React.useMemo(() => mergeStyleSets({
        wrapper: {
            marginLeft: 4,
        },
        link: {
            background: theme.semanticColors.buttonBackground,
            color: theme.semanticColors.buttonText,
            display: "inline-block",
            margin: 2,
            padding: "0 10px",
            fontSize: 14,
            textDecoration: "none",
            selectors: {
                ":hover": {
                    background: theme.palette.themePrimary,
                    color: theme.palette.white,
                    textDecoration: "none",
                },
                ":focus": {
                    color: theme.semanticColors.buttonText
                },
                ":active": {
                    color: theme.semanticColors.buttonText,
                    textDecoration: "none"
                },
                ":focus:hover": {
                    color: theme.palette.white,
                },
                ":active:hover": {
                    color: theme.palette.white,
                    textDecoration: "none"
                }
            }
        },
        active: {
            background: theme.palette.themePrimary,
            color: theme.palette.white,
            selectors: {
                ":focus": {
                    color: theme.palette.white
                }
            }
        }
    }), [theme]);

    const [logsDisabled, setLogsDisabled] = React.useState(true);
    React.useEffect(() => {
        hasLogAccess().then(response => {
            setLogsDisabled(!response);
        }).catch(() => {
            setLogsDisabled(true);
        });
    }, []);
    const logsDisabledStyle = usePivotItemDisable(logsDisabled);

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
                <Stack horizontal>
                    <Stack.Item grow={0} className={navStyles.wrapper}>
                        {subMenuItems[mainNav]?.map((row, idx) => {
                            return <Link
                                disabled={row.itemKey === "/topology/logs" && logsDisabled}
                                key={`MenuLink_${idx}`}
                                href={`#${row.itemKey}`}
                                className={[
                                    navStyles.link,
                                    row.itemKey === "/topology/logs" && logsDisabled ? logsDisabledStyle : "",
                                    row.itemKey === subNav || row.itemKey === altSubNav ? navStyles.active : ""
                                ].join(" ")}
                            >
                                {row.headerText}
                            </Link>;
                        })}
                    </Stack.Item>
                    {!subNav &&
                        <Stack.Item grow={1} style={{ lineHeight: "24px" }}>
                            {hashPath.includes("/files/")
                            ? <Breadcrumbs hashPath={hashPath} ignoreN={2} />
                            : <Breadcrumbs hashPath={hashPath} ignoreN={1} />
                            }                    
                        </Stack.Item>
                    }
                </Stack>
            </Stack.Item>
            <Stack.Item align="center" grow={0}>
                <IconButton title={nlsHPCC.History} iconProps={{ iconName: "History" }} menuProps={{ items: history }} />
                <IconButton
                    title={isFavorite ? nlsHPCC.RemoveFromFavorites : nlsHPCC.AddToFavorites}
                    iconProps={{ iconName: isFavorite ? "FavoriteStarFill" : "FavoriteStar" }}
                    menuProps={favoriteCount ? { items: favoriteMenu } : null}
                    split={favoriteCount > 0}
                    splitButtonAriaLabel={nlsHPCC.Favorites}
                    onClick={() => {
                        if (isFavorite) {
                            removeFavorite();
                        } else {
                            addFavorite();
                        }
                    }}
                    styles={{ splitButtonMenuButton: { backgroundColor: theme.palette.themeLighter, border: "none" } }}
                />
            </Stack.Item>
        </Stack>
    </div>;
};
