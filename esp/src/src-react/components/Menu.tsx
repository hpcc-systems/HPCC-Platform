import * as React from "react";
import { IconButton, IContextualMenuItem, Link, mergeStyleSets } from "@fluentui/react";
import { ToggleButton, makeStyles, tokens } from "@fluentui/react-components";
import { StackShim, StackItemShim } from "@fluentui/react-migration-v8-v9";
import { NavDrawer, NavDrawerBody, NavDrawerFooter, NavItem } from "@fluentui/react-nav-preview";
import {
    Home20Filled, Home20Regular, TextGrammarLightning20Filled, TextGrammarLightning20Regular,
    DatabaseWindow20Filled, DatabaseWindow20Regular,
    Globe20Filled, Globe20Regular,
    Organization20Filled, Organization20Regular,
    ShieldBadge20Filled, ShieldBadge20Regular,
    WeatherSunnyRegular, WeatherMoonRegular,
    bundleIcon, FluentIcon
} from "@fluentui/react-icons";
import nlsHPCC from "src/nlsHPCC";
import { containerized, bare_metal } from "src/BuildInfo";
import { navCategory } from "../util/history";
import { MainNav, routes } from "../routes";
import { useFavorite, useFavorites, useHistory } from "../hooks/favorite";
import { useLogAccessInfo } from "../hooks/platform";
import { useSessionStore } from "../hooks/store";
import { useUserTheme } from "../hooks/theme";
import { useCheckEnvAuthType, useMyAccount } from "../hooks/user";
import { Breadcrumbs } from "./Breadcrumbs";

export interface NextPrevious {
    next: () => void;
    previous: () => void;
}
export type NextPreviousT = NextPrevious | undefined;

export function useNextPrev(val?: NextPrevious): [NextPreviousT, (val: NextPrevious) => void] {
    const [nextPrev, setNextPrev] = useSessionStore<NextPreviousT>("NEXT_PREV_KEY", val, true);
    return [nextPrev, setNextPrev];
}

//  Top Level Nav  ---

const useStyles = makeStyles({
    root: {
        overflow: "hidden",
        display: "flex",
        height: "100%"
    },
    nav: {
        maxWidth: "200px",
    },
    navSmall: {
        maxWidth: "48px", // changed from 52px to 48px
        minWidth: "48px", // add this to enforce fixed width
        width: "48px",    // add this to enforce fixed width
    },
    content: {
        flex: "1",
        padding: "16px",
        display: "grid",
        justifyContent: "flex-start",
        alignItems: "flex-start",
    },
    field: {
        display: "flex",
        marginTop: "4px",
        marginLeft: "8px",
        flexDirection: "column",
        gridRowGap: tokens.spacingVerticalS,
    },
});

interface NavItemData {
    name: string;
    href: string;
    icon: FluentIcon;
    key: string;
    value: string;
}

const Home = bundleIcon(Home20Filled, Home20Regular);
const TextGrammarLightning = bundleIcon(TextGrammarLightning20Filled, TextGrammarLightning20Regular);
const DatabaseWindow = bundleIcon(DatabaseWindow20Filled, DatabaseWindow20Regular);
const Globe = bundleIcon(Globe20Filled, Globe20Regular);
const Organization = bundleIcon(Organization20Filled, Organization20Regular);
const ShieldBadge = bundleIcon(ShieldBadge20Filled, ShieldBadge20Regular);

function navLinkGroups(): NavItemData[] {
    let links: NavItemData[] = [
        {
            name: nlsHPCC.Activities,
            href: "#/activities",
            icon: Home,
            key: "activities",
            value: "activities"
        },
        {
            name: nlsHPCC.ECL,
            href: "#/workunits",
            icon: TextGrammarLightning,
            key: "workunits",
            value: "workunits"
        },
        {
            name: nlsHPCC.Files,
            href: "#/files",
            icon: DatabaseWindow,
            key: "files",
            value: "files"
        },
        {
            name: nlsHPCC.PublishedQueries,
            href: "#/queries",
            icon: Globe,
            key: "queries",
            value: "queries"
        },
        {
            name: nlsHPCC.Topology,
            href: "#/topology",
            icon: Organization,
            key: "topology",
            value: "topology"
        },
        {
            name: nlsHPCC.Operations,
            href: "#/operations",
            icon: ShieldBadge,
            key: "operations",
            value: "operations"
        }
    ];
    if (!containerized) {
        links = links.filter(l => l.key !== "topology");
    }
    if (!bare_metal) {
        links = links.filter(l => l.key !== "operations");
    }
    return links;
}

const _navIdx: { [id: string]: MainNav[] } = {};

function navIdx(id: string) {
    id = id.split("!")[0];
    if (!_navIdx[id]) {
        _navIdx[id] = [];
    }
    return _navIdx[id];
}

function append(route, path) {
    route.mainNav?.forEach(item => {
        navIdx(path).push(item);
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
    const rootPath = navIdx(`/${navCategory(hashPath)?.split("/")[1]}`);
    if (rootPath?.length) {
        return rootPath[0];
    }
    return null;
}

interface SubMenu {
    headerText: string;
    itemKey: string;
}

type SubMenuItems = { [nav: string]: SubMenu[] };

const subMenuItems: SubMenuItems = {
    "activities": [
        { headerText: nlsHPCC.Activities, itemKey: "/activities" },
        { headerText: nlsHPCC.ActivitiesLegacy, itemKey: "/activities-legacy" },
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
        { headerText: nlsHPCC.title_GetDFUWorkunits, itemKey: "/dfuworkunits" },
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
        { headerText: nlsHPCC.WUSummary, itemKey: "/topology/wu-summary" },
        { headerText: nlsHPCC.Security + " (L)", itemKey: "/topology/security" },
        { headerText: nlsHPCC.DESDL + " (L)", itemKey: "/topology/desdl" },
        { headerText: nlsHPCC.DaliAdmin, itemKey: "/topology/daliadmin" },
        { headerText: nlsHPCC.Sasha, itemKey: "/topology/sasha" },
    ],
    "operations": [
        { headerText: nlsHPCC.Topology + " (L)", itemKey: "/operations" },
        { headerText: nlsHPCC.DiskUsage + " (L)", itemKey: "/operations/diskusage" },
        { headerText: nlsHPCC.TargetClusters + " (L)", itemKey: "/operations/clusters" },
        { headerText: nlsHPCC.ClusterProcesses + " (L)", itemKey: "/operations/processes" },
        { headerText: nlsHPCC.SystemServers + " (L)", itemKey: "/operations/servers" },
        { headerText: nlsHPCC.WUSummary, itemKey: "/operations/wu-summary" },
        { headerText: nlsHPCC.Security + " (L)", itemKey: "/operations/security" },
        { headerText: nlsHPCC.DESDL + " (L)", itemKey: "/operations/desdl" },
    ],
};

const _subNavIdx: { [id: string]: string[] } = {};
for (const key in subMenuItems) {
    const subNav = subMenuItems[key];
    subNav.forEach(item => {
        if (!_subNavIdx[item.itemKey]) {
            _subNavIdx[item.itemKey] = [];
        }
        _subNavIdx[item.itemKey].push(key);
    });
}

function subNavSelectedKey(hashPath) {
    const category2 = navCategory(hashPath, 2);
    if (_subNavIdx[category2]) {
        return category2;
    }
    const category = navCategory(hashPath);
    if (_subNavIdx[category]) {
        return category;
    }
    return null;
}

interface MainNavigationProps {
    hashPath: string;
    navWideMode: boolean;
}

export const MainNavigation: React.FunctionComponent<MainNavigationProps> = ({
    hashPath,
    navWideMode
}) => {
    const styles = useStyles();

    // Add the same hooks that SubNavigation uses
    const { isAdmin } = useMyAccount();
    const envHasAuth = useCheckEnvAuthType();
    const { logsEnabled, logsStatusMessage } = useLogAccessInfo();

    const selKey = React.useMemo(() => {
        return navSelectedKey(hashPath);
    }, [hashPath]);

    const subNav = React.useMemo(() => {
        return subNavSelectedKey(hashPath);
    }, [hashPath]);

    const { setTheme, isDark } = useUserTheme();

    // Helper function to check if a sub-item is disabled
    const isSubItemDisabled = React.useCallback((itemKey: string) => {
        const restrictedRoutes = ["security"];
        if (envHasAuth) {
            restrictedRoutes.push("daliadmin", "sasha");
        }

        return (itemKey === "/topology/logs" && !logsEnabled) ||
            (restrictedRoutes.some(substring => itemKey.includes(substring)) && !isAdmin);
    }, [logsEnabled, isAdmin, envHasAuth]);

    return <div className={styles.root}>
        <NavDrawer selectedValue={selKey} open={true} type={"inline"} density="medium" className={navWideMode ? styles.nav : styles.navSmall} >
            <NavDrawerBody>
                {
                    navLinkGroups().map((item: NavItemData) => (
                        <React.Fragment key={item.key}>
                            <NavItem
                                href={item.href}
                                icon={<item.icon href={item.href} />}
                                value={item.value}
                                title={item.name}
                                style={{
                                    paddingLeft: "4px",
                                    paddingRight: "4px",
                                    color: selKey === item.value ? tokens.colorBrandForeground1 : tokens.colorNeutralForeground1,
                                }}
                            >
                                {navWideMode ? item.name : ""}
                            </NavItem>
                            {navWideMode && selKey === item.value && subMenuItems[item.value]?.length > 0 && (
                                <>
                                    {subMenuItems[item.value].map((sub) => {
                                        const isDisabled = isSubItemDisabled(sub.itemKey);
                                        const getTitle = () => {
                                            if (sub.itemKey === "/topology/logs" && !logsEnabled) {
                                                return logsStatusMessage;
                                            }
                                            return sub.headerText;
                                        };

                                        return (
                                            <NavItem
                                                key={sub.itemKey}
                                                href={isDisabled ? undefined : `#${sub.itemKey}`}
                                                value={sub.itemKey}
                                                title={getTitle()}
                                                disabled={isDisabled}
                                                style={{
                                                    padding: "4px 0 4px 32px",
                                                    color: isDisabled
                                                        ? tokens.colorNeutralForegroundDisabled
                                                        : subNav === sub.itemKey
                                                            ? tokens.colorBrandForeground2
                                                            : tokens.colorNeutralForeground1,
                                                    fontWeight: subNav === sub.itemKey ? tokens.fontWeightSemibold : tokens.fontWeightRegular,
                                                    background: "none",
                                                    textDecoration: "none",
                                                    cursor: isDisabled ? "not-allowed" : "pointer",
                                                    opacity: isDisabled ? 0.6 : 1
                                                }}
                                            >
                                                {sub.headerText}
                                            </NavItem>
                                        );
                                    })}
                                </>
                            )}
                        </React.Fragment>
                    ))
                }
            </NavDrawerBody>

            <NavDrawerFooter>
                <ToggleButton appearance="transparent" icon={isDark ? <WeatherSunnyRegular /> : <WeatherMoonRegular />} style={{ justifyContent: "flex-start", width: "100%" }} onClick={() => {
                    setTheme(isDark ? "light" : "dark");
                    const themeChangeEvent = new CustomEvent("eclwatch-theme-toggle", {
                        detail: { dark: !isDark }
                    });
                    document.dispatchEvent(themeChangeEvent);
                }} >
                    {navWideMode ? nlsHPCC.Theme : ""}
                </ToggleButton>
            </NavDrawerFooter>
        </NavDrawer>
    </div >;
};

//  Second Level Nav  ---

interface SubNavigationProps {
    hashPath: string;
}

export const SubNavigation: React.FunctionComponent<SubNavigationProps> = ({
    hashPath
}) => {

    const { theme, themeV9 } = useUserTheme();
    const { isAdmin } = useMyAccount();
    const envHasAuth = useCheckEnvAuthType();

    const [favorites] = useFavorites();
    const [favoriteCount, setFavoriteCount] = React.useState(0);
    const [isFavorite, addFavorite, removeFavorite] = useFavorite(window.location.hash);
    const [history] = useHistory();

    const [nextPrev] = useNextPrev();

    React.useEffect(() => {
        setFavoriteCount(Object.keys(favorites).length);
    }, [favorites]);

    const mainNav = React.useMemo(() => {
        return navSelectedKey(hashPath);
    }, [hashPath]);

    const subNav = React.useMemo(() => {
        return subNavSelectedKey(hashPath);
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

    const { logsEnabled, logsStatusMessage } = useLogAccessInfo();
    const linkStyle = React.useCallback((disabled) => {
        return disabled ? {
            background: themeV9.colorNeutralBackgroundDisabled,
            color: themeV9.colorNeutralForegroundDisabled
        } : {};
    }, [themeV9]);

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
        <StackShim horizontal horizontalAlign="space-between">
            <StackItemShim align="center" grow={1}>
                <StackShim horizontal>
                    <StackItemShim grow={0} className={navStyles.wrapper}>
                        {subMenuItems[mainNav]?.map((row, idx) => {
                            const restrictedRoutes = ["security"];
                            if (envHasAuth) {
                                restrictedRoutes.push("daliadmin", "sasha");
                            }
                            const linkDisabled = (row.itemKey === "/topology/logs" && !logsEnabled) || (restrictedRoutes.some(substring => row.itemKey.indexOf(substring) > -1) && !isAdmin);
                            return <Link
                                disabled={linkDisabled}
                                title={row.itemKey === "/topology/logs" && !logsEnabled ? logsStatusMessage : ""}
                                key={`MenuLink_${idx}`}
                                href={`#${row.itemKey}`}
                                className={[
                                    navStyles.link,
                                    row.itemKey === subNav ? navStyles.active : "",
                                    !subNav && row.itemKey === "/topology/configuration" ? navStyles.active : ""
                                ].join(" ")}
                                style={linkStyle(linkDisabled)}
                            >
                                {row.headerText}
                            </Link>;
                        })}
                    </StackItemShim>
                    {!!subNav &&
                        <StackItemShim grow={1} style={{ lineHeight: "24px" }}>
                            {hashPath.includes("/files/")
                                ? <Breadcrumbs hashPath={hashPath} ignoreN={2} />
                                : <Breadcrumbs hashPath={hashPath} ignoreN={1} />
                            }
                        </StackItemShim>
                    }
                </StackShim>
            </StackItemShim>
            <StackItemShim align="center" grow={0}>
                {nextPrev?.next && <IconButton title={nlsHPCC.NextWorkunit} iconProps={{ iconName: "Movers" }} onClick={() => nextPrev.next()} />}
                {nextPrev?.previous && <IconButton title={nlsHPCC.PreviousWorkunit} iconProps={{ iconName: "Sell" }} onClick={() => nextPrev.previous()} />}
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
            </StackItemShim>
        </StackShim>
    </div>;
};