import * as React from "react";
import { ContextualMenuItemType, DefaultButton, IconButton, IIconProps, Image, IPanelProps, IPersonaSharedProps, IRenderFunction, Link, mergeStyleSets, Panel, PanelType, Persona, PersonaSize, SearchBox, Stack, Text, useTheme } from "@fluentui/react";
import { useBoolean } from "@fluentui/react-hooks";
import { About } from "./About";
import { MyAccount } from "./MyAccount";

import * as WsAccount from "src/ws_account";
import * as cookie from "dojo/cookie";

import nlsHPCC from "src/nlsHPCC";
import { useBanner } from "../hooks/banner";
import { useECLWatchLogger } from "../hooks/logging";
import { useGlobalStore } from "../hooks/store";
import * as Utility from "src/Utility";
import { TitlebarConfig } from "./forms/TitlebarConfig";
import { ComingSoon } from "./controls/ComingSoon";

const collapseMenuIcon: IIconProps = { iconName: "CollapseMenu" };

const waffleIcon: IIconProps = { iconName: "WaffleOffice365" };
const searchboxStyles = { margin: "5px", height: "auto", width: "100%" };

const personaStyles = {
    root: {
        display: "flex",
        alignItems: "center",
        "&:hover": { cursor: "pointer" }
    }
};

interface DevTitleProps {
}

export const DevTitle: React.FunctionComponent<DevTitleProps> = ({
}) => {
    const theme = useTheme();
    const toolbarThemeDefaults = { active: "false", text: "", color: theme.palette.themeLight };

    const [showAbout, setShowAbout] = React.useState(false);
    const [showMyAccount, setShowMyAccount] = React.useState(false);
    const [currentUser, setCurrentUser] = React.useState(undefined);
    const [isOpen, { setTrue: openPanel, setFalse: dismissPanel }] = useBoolean(false);

    const [showTitlebarConfig, setShowTitlebarConfig] = React.useState(false);
    const [showEnvironmentTitle, setShowEnvironmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Active", toolbarThemeDefaults.active, true);
    const [environmentTitle, setEnvironmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Text", toolbarThemeDefaults.text, true);
    const [titlebarColor, setTitlebarColor] = useGlobalStore("HPCCPlatformWidget_Toolbar_Color", toolbarThemeDefaults.color, true);

    const [showBannerConfig, setShowBannerConfig] = React.useState(false);
    const [BannerMessageBar, BannerConfig] = useBanner({ showForm: showBannerConfig, setShowForm: setShowBannerConfig });

    const personaProps: IPersonaSharedProps = React.useMemo(() => {
        return {
            text: currentUser?.firstName + " " + currentUser?.lastName,
            secondaryText: currentUser?.accountType,
            size: PersonaSize.size32
        };
    }, [currentUser]);

    const onRenderNavigationContent: IRenderFunction<IPanelProps> = React.useCallback(
        (props, defaultRender) => (
            <>
                <IconButton iconProps={waffleIcon} onClick={dismissPanel} style={{ width: 48, height: 48 }} />
                <span style={searchboxStyles} />
                {defaultRender!(props)}
            </>
        ),
        [dismissPanel],
    );

    const [log] = useECLWatchLogger();

    const advMenuProps = React.useMemo(() => {
        return {
            items: [
                { key: "banner", text: nlsHPCC.SetBanner, onClick: () => setShowBannerConfig(true) },
                { key: "toolbar", text: nlsHPCC.SetToolbar, onClick: () => setShowTitlebarConfig(true) },
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
                        fetch("esp/lock", {
                            method: "post"
                        }).then(() => { window.location.href = "/esp/files/Login.html"; });
                    }
                },
                {
                    key: "logout", text: nlsHPCC.Logout, disabled: !currentUser?.username, onClick: () => {
                        fetch("esp/logout", {
                            method: "post"
                        }).then(data => {
                            if (data) {
                                cookie("ECLWatchUser", "", { expires: -1 });
                                cookie("ESPSessionID" + location.port + " = '' ", "", { expires: -1 });
                                cookie("Status", "", { expires: -1 });
                                cookie("User", "", { expires: -1 });
                                window.location.reload();
                            }
                        });
                    }
                },
                { key: "divider_3", itemType: ContextualMenuItemType.Divider },
                { key: "config", href: "#/topology/configuration", text: nlsHPCC.Configuration },
                { key: "about", text: nlsHPCC.About, onClick: () => setShowAbout(true) }
            ],
            directionalHintFixed: true
        };
    }, [currentUser?.username]);

    const btnStyles = mergeStyleSets({
        errorsWarnings: {
            border: "none",
            background: "transparent",
            minWidth: 48,
            padding: "0 10px 0 4px",
            color: theme.semanticColors.link
        },
        errorsWarningsCount: {
            margin: "-3px 0 0 -3px"
        }
    });

    React.useEffect(() => {
        WsAccount.MyAccount({})
            .then(({ MyAccountResponse }) => {
                setCurrentUser(MyAccountResponse);
            })
            ;
    }, [setCurrentUser]);

    React.useEffect(() => {
        if (!environmentTitle) return;
        document.title = environmentTitle;
    }, [environmentTitle]);

    return <div style={{ backgroundColor: titlebarColor ?? theme.palette.themeLight }}>
        <BannerMessageBar />
        <Stack horizontal verticalAlign="center" horizontalAlign="space-between">
            <Stack.Item align="center">
                <Stack horizontal>
                    <Stack.Item>
                        <IconButton iconProps={waffleIcon} onClick={openPanel} style={{ width: 48, height: 48, color: theme.palette.themeDarker }} />
                    </Stack.Item>
                    <Stack.Item align="center">
                        <Link href="#/activities">
                            <Text variant="large" nowrap block >
                                <b style={{ color: (titlebarColor !== toolbarThemeDefaults.color) ? Utility.textColor(titlebarColor) : theme.palette.themeDarker }}>
                                    ECL Watch - 9{environmentTitle !== "" && showEnvironmentTitle ? ` | ${environmentTitle}` : ""}
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
                    <Stack.Item align="center" >
                        <ComingSoon defaultValue style={{ color: titlebarColor !== toolbarThemeDefaults.color ? Utility.textColor(titlebarColor) : theme.palette.themeDarker }} />
                    </Stack.Item>
                    <Stack.Item align="center">
                        <DefaultButton href="#/log" title={nlsHPCC.ErrorWarnings} iconProps={{ iconName: log.length > 0 ? "RingerSolid" : "Ringer" }} className={btnStyles.errorsWarnings}>
                            <span className={btnStyles.errorsWarningsCount}>{`(${log.length})`}</span>
                        </DefaultButton>
                    </Stack.Item>
                    <Stack.Item align="center">
                        <IconButton title={nlsHPCC.Advanced} iconProps={collapseMenuIcon} menuProps={advMenuProps} />
                    </Stack.Item>
                </Stack>
            </Stack.Item>
        </Stack>
        <Panel type={PanelType.smallFixedNear}
            onRenderNavigationContent={onRenderNavigationContent}
            headerText={nlsHPCC.Apps}
            isLightDismiss
            isOpen={isOpen}
            onDismiss={dismissPanel}
            hasCloseButton={false}
        >
            <DefaultButton text="Kibana" href="https://www.elastic.co/kibana/" target="_blank" onRenderIcon={() => <Image src="https://www.google.com/s2/favicons?domain=www.elastic.co" />} />
            <DefaultButton text="K8s Dashboard" href="https://kubernetes.io/docs/tasks/access-application-cluster/web-ui-dashboard/" target="_blank" onRenderIcon={() => <Image src="https://www.google.com/s2/favicons?domain=kubernetes.io" />} />
        </Panel>
        <About eclwatchVersion="9" show={showAbout} onClose={() => setShowAbout(false)} ></About>
        <MyAccount currentUser={currentUser} show={showMyAccount} onClose={() => setShowMyAccount(false)}></MyAccount>
        <TitlebarConfig
            toolbarThemeDefaults={toolbarThemeDefaults}
            showForm={showTitlebarConfig} setShowForm={setShowTitlebarConfig}
            showEnvironmentTitle={showEnvironmentTitle} setShowEnvironmentTitle={setShowEnvironmentTitle}
            environmentTitle={environmentTitle} setEnvironmentTitle={setEnvironmentTitle}
            titlebarColor={titlebarColor} setTitlebarColor={setTitlebarColor}
        />
        <BannerConfig />
    </div>;
};

