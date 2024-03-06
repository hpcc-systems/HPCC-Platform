import * as React from "react";
import { ContextualMenuItemType, DefaultButton, IconButton, IContextualMenuItem, IIconProps, IPersonaSharedProps, Link, mergeStyleSets, Persona, PersonaSize, SearchBox, Stack, Text, ThemeProvider, } from "@fluentui/react";
import { CounterBadgeProps, CounterBadge, FluentProvider } from "@fluentui/react-components";
import { Level, scopedLogger } from "@hpcc-js/util";
import { useBoolean } from "@fluentui/react-hooks";
import { Toaster } from "react-hot-toast";
import { cookie } from "dojo/main";

import nlsHPCC from "src/nlsHPCC";
import * as Utility from "src/Utility";

import { useBanner } from "../hooks/banner";
import { useECLWatchLogger } from "../hooks/logging";
import { useBuildInfo, useModernMode } from "../hooks/platform";
import { useGlobalStore } from "../hooks/store";
import { useMyAccount, useUserSession } from "../hooks/user";
import { replaceUrl } from "../util/history";
import { useCheckFeatures } from "../hooks/platform";
import { useUserTheme, useToolbarTheme } from "../hooks/theme";

import { TitlebarConfig } from "./forms/TitlebarConfig";
import { switchTechPreview } from "./controls/ComingSoon";
import { About } from "./About";
import { MyAccount } from "./MyAccount";
import { toasterScale } from "./controls/CustomToaster";

const logger = scopedLogger("src-react/components/Title.tsx");
import { AppPanel } from "./AppPanel";

const collapseMenuIcon: IIconProps = { iconName: "CollapseMenu" };

const waffleIcon: IIconProps = { iconName: "WaffleOffice365" };

const personaStyles = {
    root: {
        display: "flex",
        alignItems: "center",
        "&:hover": { cursor: "pointer" }
    }
};

const DAY = 1000 * 60 * 60 * 24;

interface DevTitleProps {
}

export const DevTitle: React.FunctionComponent<DevTitleProps> = ({
}) => {

    const [, { opsCategory }] = useBuildInfo();
    const { themeV9 } = useUserTheme();
    const { toolbarTheme, toolbarThemeV9 } = useToolbarTheme();
    const { userSession, setUserSession, deleteUserSession } = useUserSession();
    const toolbarThemeDefaults = { active: false, text: "", color: themeV9.colorBrandBackground };
    const [logIconColor, setLogIconColor] = React.useState<CounterBadgeProps["color"]>();

    const [showAbout, setShowAbout] = React.useState(false);
    const [showMyAccount, setShowMyAccount] = React.useState(false);
    const { currentUser, isAdmin } = useMyAccount();
    const [showAppPanel, { setTrue: openAppPanel, setFalse: dismissAppPanel }] = useBoolean(false);

    const [showTitlebarConfig, setShowTitlebarConfig] = React.useState(false);
    const [showEnvironmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Active", toolbarThemeDefaults.active, true);
    const [environmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Text", toolbarThemeDefaults.text, true);

    const [showBannerConfig, setShowBannerConfig] = React.useState(false);
    const [BannerMessageBar, BannerConfig] = useBanner({ showForm: showBannerConfig, setShowForm: setShowBannerConfig });

    const personaProps: IPersonaSharedProps = React.useMemo(() => {
        return {
            text: (currentUser?.firstName && currentUser?.lastName) ? currentUser.firstName + " " + currentUser.lastName : currentUser?.username,
            secondaryText: currentUser?.accountType,
            size: PersonaSize.size32
        };
    }, [currentUser]);

    const [log, logLastUpdated] = useECLWatchLogger();

    const { setModernMode } = useModernMode();
    const onTechPreviewClick = React.useCallback(
        (ev?: React.MouseEvent<HTMLButtonElement>, item?: IContextualMenuItem): void => {
            setModernMode(String(false));
            switchTechPreview(false, opsCategory);
        },
        [opsCategory, setModernMode]
    );

    const [logCount, setLogCount] = React.useState(0);
    React.useEffect(() => {
        const errorCodes = [Level.alert, Level.critical, Level.emergency, Level.error];
        const warningCodes = [Level.warning];
        const errorAndWarningCodes = errorCodes.concat(warningCodes);

        const logCounts = {
            errors: log.filter(msg => errorCodes.includes(msg.level)).length,
            warnings: log.filter(msg => warningCodes.includes(msg.level)).length,
            other: log.filter(msg => !errorAndWarningCodes.includes(msg.level)).length,
        };

        let count = 0;
        let color: CounterBadgeProps["color"];

        if (logCounts.errors > count) {
            count = logCounts.errors;
            color = "danger";
        } else if (logCounts.warnings > count) {
            count = logCounts.warnings;
            color = "important";
        } else {
            count = logCounts.other;
            color = "informative";
        }

        setLogCount(count);
        setLogIconColor(color);
    }, [log, logLastUpdated]);

    const advMenuProps = React.useMemo(() => {
        return {
            items: [
                { key: "banner", text: nlsHPCC.SetBanner, disabled: !isAdmin, onClick: () => setShowBannerConfig(true) },
                { key: "toolbar", text: nlsHPCC.SetToolbar, disabled: !isAdmin, onClick: () => setShowTitlebarConfig(true) },
                { key: "divider_1", itemType: ContextualMenuItemType.Divider },
                { key: "docs", href: "https://hpccsystems.com/training/documentation/", text: nlsHPCC.Documentation, target: "_blank" },
                { key: "downloads", href: "https://hpccsystems.com/download", text: nlsHPCC.Downloads, target: "_blank" },
                { key: "releaseNotes", href: "https://hpccsystems.com/download/release-notes", text: nlsHPCC.ReleaseNotes, target: "_blank" },
                {
                    key: "additionalResources", text: nlsHPCC.AdditionalResources, subMenuProps: {
                        items: [
                            { key: "redBook", href: "https://wiki.hpccsystems.com/display/hpcc/HPCC+Systems+Red+Book", text: nlsHPCC.RedBook, target: "_blank" },
                            { key: "forums", href: "https://hpccsystems.com/bb/", text: nlsHPCC.Forums, target: "_blank" },
                            { key: "issues", href: "https://track.hpccsystems.com/issues/", text: nlsHPCC.IssueReporting, target: "_blank" },
                        ]
                    }
                },
                { key: "divider_2", itemType: ContextualMenuItemType.Divider },
                {
                    key: "lock", text: nlsHPCC.Lock, disabled: !currentUser?.username, onClick: () => {
                        fetch("/esp/lock", {
                            method: "post"
                        }).then(() => {
                            setUserSession({ ...userSession, Status: "Locked" });
                            replaceUrl("/login", true);
                        });
                    }
                },
                {
                    key: "logout", text: nlsHPCC.Logout, disabled: !currentUser?.username, onClick: () => {
                        fetch("/esp/logout", {
                            method: "post"
                        }).then(data => {
                            if (data) {
                                deleteUserSession().then(() => {
                                    Utility.deleteCookie("ECLWatchUser");
                                    Utility.deleteCookie("ESPSessionID");
                                    Utility.deleteCookie("Status");
                                    Utility.deleteCookie("User");
                                    Utility.deleteCookie("ESPSessionState");
                                    window.location.reload();
                                });
                            }
                        });
                    }
                },
                { key: "divider_3", itemType: ContextualMenuItemType.Divider },
                { key: "config", href: "#/topology/configuration", text: nlsHPCC.Configuration },
                {
                    key: "eclwatchv9", text: "ECL Watch v9",
                    canCheck: true,
                    isChecked: true,
                    onClick: onTechPreviewClick
                },
                { key: "about", text: nlsHPCC.About, onClick: () => setShowAbout(true) }
            ],
            directionalHintFixed: true
        };
    }, [currentUser?.username, deleteUserSession, isAdmin, onTechPreviewClick, setUserSession, userSession]);

    const btnStyles = React.useMemo(() => mergeStyleSets({
        errorsWarnings: {
            border: "none",
            background: "transparent",
            minWidth: 48,
            padding: "0 10px 0 4px",
            color: toolbarThemeV9.colorBrandForegroundLink
        },
        errorsWarningsCount: {
            margin: "-3px 0 0 -3px"
        }
    }), [toolbarThemeV9.colorBrandForegroundLink]);

    React.useEffect(() => {
        switch (log.reduce((prev, cur) => Math.max(prev, cur.level), Level.debug)) {
            case Level.alert:
            case Level.critical:
            case Level.emergency:
                setLogIconColor("danger");
                break;
            case Level.error:
                setLogIconColor("danger");
                break;
            case Level.warning:
                setLogIconColor("important");
                break;
            case Level.info:
            case Level.notice:
            case Level.debug:
            default:
                setLogIconColor("informative");
                break;
        }
    }, [log, logLastUpdated, toolbarThemeV9]);

    const features = useCheckFeatures();

    React.useEffect(() => {
        if (!features.timestamp) return;
        const age = Math.floor((Date.now() - features.timestamp.getTime()) / DAY);
        const message = nlsHPCC.PlatformBuildIsNNNDaysOld.replace("NNN", `${age}`);
        if (age > 90) {
            logger.alert(message + `  ${nlsHPCC.PleaseUpgradeToLaterPointRelease}`);
        } else if (age > 60) {
            logger.error(message + `  ${nlsHPCC.PleaseUpgradeToLaterPointRelease}`);
        } else if (age > 30) {
            logger.warning(message + `  ${nlsHPCC.PleaseUpgradeToLaterPointRelease}`);
        } else {
            logger.info(message);
        }
    }, [features.timestamp]);

    React.useEffect(() => {
        if (!currentUser.username) return;
        if (!cookie("PasswordExpiredCheck")) {
            // cookie expires option expects whole number of days, use a decimal < 1 for hours
            cookie("PasswordExpiredCheck", "true", { expires: 0.5 });
            if (currentUser.passwordIsExpired) {
                alert(nlsHPCC.PasswordExpired);
                setShowMyAccount(true);
            } else if (currentUser.passwordDaysRemaining && currentUser.passwordDaysRemaining <= currentUser.passwordExpirationWarningDays) {
                if (confirm(nlsHPCC.PasswordExpirePrefix + currentUser.passwordDaysRemaining + nlsHPCC.PasswordExpirePostfix)) {
                    setShowMyAccount(true);
                }
            }
        }
    }, [currentUser]);

    return <div>
        <FluentProvider theme={toolbarThemeV9}>
            <ThemeProvider theme={toolbarTheme}>
                <div style={{ backgroundColor: toolbarThemeV9.colorBrandBackground2 }}>
                    <BannerMessageBar />
                    <Stack horizontal verticalAlign="center" horizontalAlign="space-between">
                        <Stack.Item align="center">
                            <Stack horizontal>
                                <Stack.Item>
                                    <IconButton iconProps={waffleIcon} onClick={openAppPanel} style={{ width: 48, height: 48 }} />
                                </Stack.Item>
                                <Stack.Item align="center">
                                    <Link href="#/activities">
                                        <Text variant="large" nowrap block >
                                            <b title="ECL Watch" style={{ color: toolbarThemeV9.colorBrandForegroundLink }}>
                                                {showEnvironmentTitle && environmentTitle.length ? environmentTitle : "ECL Watch"}
                                            </b>
                                        </Text>
                                    </Link>
                                </Stack.Item>
                            </Stack>
                        </Stack.Item>
                        <Stack.Item align="center">
                            <SearchBox onSearch={newValue => { window.location.href = `#/search/${newValue.trim()}`; }} placeholder={nlsHPCC.PlaceholderFindText} styles={{ root: { minWidth: 320 } }} />
                        </Stack.Item>
                        <Stack.Item align="center" >
                            <Stack horizontal>
                                {currentUser?.username &&
                                    <Stack.Item styles={personaStyles}>
                                        <Persona {...personaProps} onClick={() => setShowMyAccount(true)} />
                                    </Stack.Item>
                                }
                                <Stack.Item align="center">
                                    <DefaultButton href="#/log" title={nlsHPCC.ErrorWarnings} iconProps={{ iconName: logCount > 0 ? "RingerSolid" : "Ringer" }} className={btnStyles.errorsWarnings}>
                                        <CounterBadge appearance="filled" size="small" color={logIconColor} count={logCount} />
                                    </DefaultButton>
                                </Stack.Item>
                                <Stack.Item align="center">
                                    <IconButton title={nlsHPCC.Advanced} iconProps={collapseMenuIcon} menuProps={advMenuProps} />
                                </Stack.Item>
                            </Stack>
                            <Toaster position="top-right" gutter={8 - (90 - toasterScale(90))} containerStyle={{
                                top: toasterScale(57),
                                right: 8 - (180 - toasterScale(180))
                            }} />
                        </Stack.Item>
                    </Stack>
                </div>
            </ThemeProvider>
        </FluentProvider>
        <AppPanel show={showAppPanel} onDismiss={dismissAppPanel} />
        <About eclwatchVersion="9" show={showAbout} onClose={() => setShowAbout(false)} ></About>
        <MyAccount currentUser={currentUser} show={showMyAccount} onClose={() => setShowMyAccount(false)}></MyAccount>
        <TitlebarConfig toolbarThemeDefaults={toolbarThemeDefaults} showForm={showTitlebarConfig} setShowForm={setShowTitlebarConfig} />
        <BannerConfig />
    </div>;
};

