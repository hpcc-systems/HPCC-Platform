import * as React from "react";
import { Button, ButtonProps, CounterBadgeProps, CounterBadge, Link, Menu, MenuDivider, MenuItem, MenuList, MenuPopover, MenuTrigger, Persona, SearchBox, Text, Toaster, makeStyles, tokens } from "@fluentui/react-components";
import { Alert24Filled, Alert24Regular, CheckmarkRegular, GridDotsRegular, Navigation24Regular, WindowNewRegular } from "@fluentui/react-icons";
import { Level, scopedLogger } from "@hpcc-js/util";
import { cookie } from "src-dojo/index";

import nlsHPCC from "src/nlsHPCC";
import * as Utility from "src/Utility";

import { useBanner } from "../hooks/banner";
import { useConfirm } from "../hooks/confirm";
import { replaceUrl } from "../util/history";
import { useECLWatchLogger } from "../hooks/logging";
import { useBuildInfo, useModernMode, useCheckFeatures } from "../hooks/platform";
import { cmake_build_type } from "src/BuildInfo";
import { useGlobalStore } from "../hooks/store";
import { PasswordStatus, useMyAccount, useUserSession } from "../hooks/user";

import { TitlebarConfig } from "./forms/TitlebarConfig";
import { switchTechPreview } from "./controls/ComingSoon";
import { About } from "./About";
import { MyAccount } from "./MyAccount";
import { LogViewerDialog } from "./LogViewerDialog";
import { ColorTokens } from "./ColorTokens";
import { debounce } from "../util/throttle";

const logger = scopedLogger("src-react/components/Title.tsx");

const NewTabButton: React.FunctionComponent<ButtonProps> = (props) => {
    return <Button
        {...props}
        appearance="transparent"
        icon={<WindowNewRegular />}
        size="small"
    />;
};

const useStyles = makeStyles({
    personaWrapper: {
        display: "flex",
        alignItems: "center",
        "& span": {
            lineHeight: tokens.lineHeightBase600
        }
    },
});

const useBtnStyles = makeStyles({
    errorsWarnings: {
        border: "none",
        background: "transparent",
        minWidth: "48px",
        padding: "0 10px 0 4px",
    },
});

const DAY = 1000 * 60 * 60 * 24;

interface DevTitleProps {
    setNavWideMode: React.Dispatch<React.SetStateAction<boolean>>;
    navWideMode: boolean;
}

export const DevTitle: React.FunctionComponent<DevTitleProps> = ({
    setNavWideMode,
    navWideMode
}) => {

    const [, { opsCategory }] = useBuildInfo();
    const { userSession, setUserSession, deleteUserSession } = useUserSession();
    const [logIconColor, setLogIconColor] = React.useState<CounterBadgeProps["color"]>();

    const [showAbout, setShowAbout] = React.useState(false);
    const [searchValue, setSearchValue] = React.useState("");
    const [showMyAccount, setShowMyAccount] = React.useState(false);
    const [showLogViewer, setShowLogViewer] = React.useState(false);
    const { currentUser, isAdmin } = useMyAccount();

    const [showTitlebarConfig, setShowTitlebarConfig] = React.useState(false);
    const [showColourTokens, setShowColourTokens] = React.useState(false);
    const [showEnvironmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Active", false, true);
    const [environmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Text", undefined, true);
    const [titlebarColor] = useGlobalStore("HPCCPlatformWidget_Toolbar_Color", undefined, true);

    const [showBannerConfig, setShowBannerConfig] = React.useState(false);
    const [BannerMessageBar, BannerConfig] = useBanner({ showForm: showBannerConfig, setShowForm: setShowBannerConfig });

    const styles = useStyles();

    const [PasswordExpiredConfirm, setPasswordExpiredConfirm] = useConfirm({
        title: nlsHPCC.PasswordExpiration,
        message: nlsHPCC.PasswordExpired,
        cancelLabel: null,
        onSubmit: React.useCallback(() => {
            setShowMyAccount(true);
        }, [])
    });

    const onSearchKeyUp = debounce((evt) => {
        if (evt.key === "Enter") {
            if (!evt.target.value) return;
            if (evt.ctrlKey) {
                window.open(`#/search/${searchValue.trim()}`);
            } else {
                window.location.assign(`#/search/${searchValue.trim()}`);
            }
        } else {
            setSearchValue(evt.target.value);
        }
    }, 100);

    const onSearchNewTabClick = React.useCallback(() => {
        if (!searchValue) return;
        window.open(`#/search/${searchValue.trim()}`);
    }, [searchValue]);

    const titlebarColorSet = React.useMemo(() => {
        return !!titlebarColor;
    }, [titlebarColor]);

    const personaProps = React.useMemo(() => {
        return {
            name: (currentUser?.firstName && currentUser?.lastName) ? currentUser.firstName + " " + currentUser.lastName : currentUser?.username,
            secondaryText: currentUser?.accountType,
            size: "medium" as const
        };
    }, [currentUser]);

    const { id: toasterId, log, lastUpdate: logLastUpdated } = useECLWatchLogger();

    const { setModernMode } = useModernMode();
    const onTechPreviewClick = React.useCallback(
        (): void => {
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

    const onLockClick = React.useCallback(() => {
        fetch("/esp/lock", { method: "post" }).then(() => {
            setUserSession({ ...userSession });
            replaceUrl("/login", true);
        });
    }, [setUserSession, userSession]);

    const onLogoutClick = React.useCallback(() => {
        fetch("/esp/logout", { method: "post" }).then(data => {
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
    }, [deleteUserSession]);

    const btnStyles = useBtnStyles();
    const btnColor = titlebarColor ? Utility.textColor(titlebarColor) : tokens.colorBrandForegroundLink;

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
    }, [log, logLastUpdated]);

    const features = useCheckFeatures();

    React.useEffect(() => {
        if (!features.timestamp) return;
        let ancient = 90;
        let veryOld = 60;
        let old = 30;
        if (features.maturity === "trunk") {
            ancient = 360;
            veryOld = 180;
            old = 90;
        } else if (features.maturity === "rc") {
            ancient = 28;
            veryOld = 21;
            old = 14;
        }
        const age = Math.floor((Date.now() - features.timestamp.getTime()) / DAY);
        const message = nlsHPCC.PlatformBuildIsNNNDaysOld.replace("NNN", `${age}`);
        if (age > ancient) {
            logger.alert(message + `  ${nlsHPCC.PleaseUpgradeToLaterPointRelease}`);
        } else if (age > veryOld) {
            logger.error(message + `  ${nlsHPCC.PleaseUpgradeToLaterPointRelease}`);
        } else if (age > old) {
            logger.warning(message + `  ${nlsHPCC.PleaseUpgradeToLaterPointRelease}`);
        } else {
            logger.info(message);
        }
    }, [features.maturity, features.timestamp]);

    React.useEffect(() => {
        if (!currentUser.username) return;
        if (!cookie("PasswordExpiredCheck")) {
            // cookie expires option expects whole number of days, use a decimal < 1 for hours
            cookie("PasswordExpiredCheck", "true", { expires: 0.5, path: "/" });
            switch (currentUser.passwordDaysRemaining) {
                case PasswordStatus.Expired:
                    setPasswordExpiredConfirm(true);
                    break;
                case PasswordStatus.NeverExpires:
                case null:
                    break;
                default:
                    if (currentUser?.passwordDaysRemaining <= currentUser?.passwordExpirationWarningDays) {
                        if (confirm(nlsHPCC.PasswordExpirePrefix + currentUser.passwordDaysRemaining + nlsHPCC.PasswordExpirePostfix)) {
                            setShowMyAccount(true);
                        }
                    }
                    break;
            }
        }
    }, [currentUser, setPasswordExpiredConfirm]);

    return <div style={{ backgroundColor: titlebarColorSet ? titlebarColor : tokens.colorBrandBackground2Hover }}>
        <BannerMessageBar />
        <div style={{ display: "flex", flexDirection: "row", alignItems: "center", justifyContent: "space-between" }}>
            <div style={{ alignSelf: "center" }}>
                <div style={{ display: "flex", flexDirection: "row" }}>
                    <div style={{ width: 48, height: 48, display: "flex", justifyContent: "center", alignItems: "center" }}>
                        <Button appearance="transparent" icon={<GridDotsRegular />} onClick={() => setNavWideMode(!navWideMode)} style={{ color: titlebarColorSet ? Utility.textColor(titlebarColor) : tokens.colorBrandForeground2 }} />
                    </div>
                    <div style={{ alignSelf: "center" }}>
                        <Link href="#/activities">
                            <Text size={400} block truncate wrap={false}>
                                <b title="ECL Watch" style={{ paddingLeft: "8px", color: titlebarColorSet ? Utility.textColor(titlebarColor) : tokens.colorBrandForeground2 }}>
                                    {(showEnvironmentTitle && environmentTitle) ? environmentTitle : "ECL Watch"}
                                </b>
                            </Text>
                        </Link>
                    </div>
                </div>
            </div>
            <div style={{ alignSelf: "center" }}>
                <SearchBox onKeyUp={onSearchKeyUp} contentAfter={<NewTabButton onClick={onSearchNewTabClick} />} placeholder={nlsHPCC.PlaceholderFindText} style={{ minWidth: 320 }} />
            </div>
            <div style={{ alignSelf: "center" }}>
                <div style={{ display: "flex", flexDirection: "row" }}>
                    {currentUser?.username &&
                        <div className={styles.personaWrapper}>
                            <div onClick={() => setShowMyAccount(true)} style={{ cursor: "pointer" }}>
                                <Persona {...personaProps} />
                            </div>
                        </div>
                    }
                    <div style={{ alignSelf: "center" }}>
                        <Button size="small" appearance="transparent" onClick={() => setShowLogViewer(true)} title={nlsHPCC.ErrorWarnings} icon={logCount > 0 ? <Alert24Filled /> : <Alert24Regular />} className={btnStyles.errorsWarnings} style={{ color: btnColor }}>
                            <CounterBadge appearance="filled" size="small" color={logIconColor} count={logCount} />
                        </Button>
                    </div>
                    {/* <div style={{ alignSelf: "center" }}> */}
                    <div style={{ width: 40, height: 48, display: "flex", justifyContent: "center", alignItems: "center" }}>
                        <Menu>
                            <MenuTrigger disableButtonEnhancement>
                                <Button appearance="transparent" icon={<Navigation24Regular />} title={nlsHPCC.Advanced} style={{ color: titlebarColorSet ? Utility.textColor(titlebarColor) : tokens.colorBrandForeground2 }} />
                            </MenuTrigger>
                            <MenuPopover>
                                <MenuList>
                                    <MenuItem disabled={currentUser?.username !== "" && !isAdmin} onClick={() => setShowBannerConfig(true)}>{nlsHPCC.SetBanner}</MenuItem>
                                    <MenuItem disabled={currentUser?.username !== "" && !isAdmin} onClick={() => setShowTitlebarConfig(true)}>{nlsHPCC.SetToolbar}</MenuItem>
                                    <MenuDivider />
                                    <MenuItem onClick={() => window.open("https://hpccsystems.com/training/documentation/", "_blank", "noopener")}>{nlsHPCC.Documentation}</MenuItem>
                                    <MenuItem onClick={() => window.open("https://hpccsystems.com/download", "_blank", "noopener")}>{nlsHPCC.Downloads}</MenuItem>
                                    <MenuItem onClick={() => window.open("https://hpccsystems.com/download/release-notes", "_blank", "noopener")}>{nlsHPCC.ReleaseNotes}</MenuItem>
                                    <Menu>
                                        <MenuTrigger disableButtonEnhancement>
                                            <MenuItem>{nlsHPCC.AdditionalResources}</MenuItem>
                                        </MenuTrigger>
                                        <MenuPopover>
                                            <MenuList>
                                                <MenuItem onClick={() => window.open("https://hpcc-systems.github.io/HPCC-Platform/devdoc/red_book/HPCC-Systems-Red-Book.html", "_blank", "noopener")}>{nlsHPCC.RedBook}</MenuItem>
                                                <MenuItem onClick={() => window.open("https://hpccsystems.com/bb/", "_blank", "noopener")}>{nlsHPCC.Forums}</MenuItem>
                                                <MenuItem onClick={() => window.open("https://hpccsystems.atlassian.net/issues/", "_blank", "noopener")}>{nlsHPCC.IssueReporting}</MenuItem>
                                            </MenuList>
                                        </MenuPopover>
                                    </Menu>
                                    <MenuDivider />
                                    <MenuItem disabled={!currentUser?.username} onClick={onLockClick}>{nlsHPCC.Lock}</MenuItem>
                                    <MenuItem disabled={!currentUser?.username} onClick={onLogoutClick}>{nlsHPCC.Logout}</MenuItem>
                                    <MenuDivider />
                                    <MenuItem onClick={() => { window.location.href = "#/topology/configuration"; }}>{nlsHPCC.Configuration}</MenuItem>
                                    <MenuItem icon={<CheckmarkRegular />} onClick={onTechPreviewClick}>ECL Watch v9</MenuItem>
                                    <MenuDivider />
                                    <MenuItem onClick={() => { window.location.href = "/esp/files/index.html#/reset"; }}>{nlsHPCC.ResetUserSettings}</MenuItem>
                                    <MenuItem onClick={() => setShowAbout(true)}>{nlsHPCC.About}</MenuItem>
                                    {cmake_build_type === "Debug" && <MenuDivider />}
                                    {cmake_build_type === "Debug" && <MenuItem onClick={() => setShowColourTokens(true)}>{nlsHPCC.ColorTokens}</MenuItem>}
                                </MenuList>
                            </MenuPopover>
                        </Menu>
                    </div>
                </div>
                <Toaster toasterId={toasterId} position={"top-end"} pauseOnHover />
            </div>
        </div>
        <About eclwatchVersion="9" show={showAbout} onClose={() => setShowAbout(false)} ></About>
        <ColorTokens show={showColourTokens} onClose={() => setShowColourTokens(false)} />
        <MyAccount currentUser={currentUser} show={showMyAccount} onClose={() => setShowMyAccount(false)}></MyAccount>
        <LogViewerDialog show={showLogViewer} onClose={() => setShowLogViewer(false)} />
        <TitlebarConfig showForm={showTitlebarConfig} setShowForm={setShowTitlebarConfig} />
        <BannerConfig />
        <PasswordExpiredConfirm />
    </div>;
};

