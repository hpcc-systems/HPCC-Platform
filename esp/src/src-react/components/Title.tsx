import * as React from "react";
import { ContextualMenuItemType, DefaultButton, IconButton, IIconProps, Image, IPanelProps, IPersonaSharedProps, IRenderFunction, Link, Panel, PanelType, Persona, PersonaSize, SearchBox, Stack, Text, useTheme } from "@fluentui/react";
import { About } from "./About";
import { useBoolean } from "@fluentui/react-hooks";

import nlsHPCC from "src/nlsHPCC";
import { useECLWatchLogger } from "../hooks/logging";

const collapseMenuIcon: IIconProps = { iconName: "CollapseMenu" };

const waffleIcon: IIconProps = { iconName: "WaffleOffice365" };
const searchboxStyles = { margin: "5px", height: "auto", width: "100%" };

const examplePersona: IPersonaSharedProps = {
    secondaryText: "Designer",
    tertiaryText: "In a meeting",
    optionalText: "Available at 4:00pm",
};

interface DevTitleProps {
}

export const DevTitle: React.FunctionComponent<DevTitleProps> = ({
}) => {

    const [showAbout, setShowAbout] = React.useState(false);
    const [isOpen, { setTrue: openPanel, setFalse: dismissPanel }] = useBoolean(false);

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
                { key: "legacy", text: nlsHPCC.OpenLegacyECLWatch, href: "/esp/files/stub.htm" },
                { key: "divider_0", itemType: ContextualMenuItemType.Divider },
                { key: "errors", href: "#/log", text: `${nlsHPCC.ErrorWarnings} ${log.length > 0 ? `(${log.length})` : ""}`, },
                { key: "divider_1", itemType: ContextualMenuItemType.Divider },
                { key: "banner", text: nlsHPCC.SetBanner },
                { key: "toolbar", text: nlsHPCC.SetToolbar },
                { key: "divider_2", itemType: ContextualMenuItemType.Divider },
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
                { key: "divider_3", itemType: ContextualMenuItemType.Divider },
                { key: "lock", text: nlsHPCC.Lock },
                { key: "logout", text: nlsHPCC.Logout },
                { key: "divider_4", itemType: ContextualMenuItemType.Divider },
                { key: "config", href: "#/config", text: nlsHPCC.Configuration },
                { key: "about", text: nlsHPCC.About, onClick: () => setShowAbout(true) }
            ],
            directionalHintFixed: true
        };
    }, [log.length]);

    const theme = useTheme();

    return <div style={{ backgroundColor: theme.palette.themeLight }}>
        <Stack horizontal verticalAlign="center" horizontalAlign="space-between">
            <Stack.Item align="center">
                <Stack horizontal>
                    <Stack.Item>
                        <IconButton iconProps={waffleIcon} onClick={openPanel} style={{ width: 48, height: 48, color: theme.palette.themeDarker }} />
                    </Stack.Item>
                    <Stack.Item align="center">
                        <Link href="#/activities"><Text variant="large" nowrap block ><b style={{ color: theme.palette.themeDarker }}>ECL Watch</b></Text></Link>
                    </Stack.Item>
                </Stack>
            </Stack.Item>
            <Stack.Item align="center">
                <SearchBox onSearch={newValue => { window.location.href = `#/search/${newValue.trim()}`; }} placeholder={nlsHPCC.PlaceholderFindText} styles={{ root: { minWidth: 320 } }} />
            </Stack.Item>
            <Stack.Item align="center" >
                <Stack horizontal>
                    <Stack.Item>
                        <Persona {...examplePersona} text="Jane Doe" size={PersonaSize.size32} />
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
        <About show={showAbout} onClose={() => setShowAbout(false)} ></About>
    </div>;
};

