import * as React from "react";
import { Breadcrumb, ContextualMenuItemType, DefaultPalette, FontSizes, IBreadcrumbItem, IBreadcrumbStyleProps, IBreadcrumbStyles, IconButton, IContextualMenuProps, IIconProps, Image, IStyleFunctionOrObject, Link, SearchBox, Stack, Toggle } from "@fluentui/react";
import { About } from "./About";

import nlsHPCC from "src/nlsHPCC";

const breadCrumbStyles: IStyleFunctionOrObject<IBreadcrumbStyleProps, IBreadcrumbStyles> = {
    itemLink: { fontSize: FontSizes.size10, lineHeight: 14, paddingLeft: 2, paddingRight: 2 },
};

const collapseMenuIcon: IIconProps = { iconName: "CollapseMenu" };

interface DevTitleProps {
    paths: string[],
    useDarkMode: boolean,
    setUseDarkMode: (_: boolean) => void;
}

export const DevTitle: React.FunctionComponent<DevTitleProps> = ({
    paths,
    useDarkMode,
    setUseDarkMode
}) => {

    const [showAbout, setShowAbout] = React.useState(false);

    let fullPath = "#";
    const itemsWithHref = [{ text: "HOME", key: "home", href: "#/" },
    ...paths.filter(path => !!path).map((path, idx) => {
        const retVal: IBreadcrumbItem = { text: path.toUpperCase(), key: "" + idx, href: `${fullPath}/${path}` };
        fullPath = `${fullPath}/${path}`;
        return retVal;
    })];

    const advMenuProps: IContextualMenuProps = {
        items: [
            { key: "errors", href: "#/errors", text: nlsHPCC.ErrorWarnings, },
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
            { key: "legacy", text: nlsHPCC.OpenLegacyECLWatch, href: "/esp/files/stub.htm" },
            { key: "divider_5", itemType: ContextualMenuItemType.Divider },
            { key: "config", href: "#/config", text: nlsHPCC.Configuration },
            { key: "about", text: nlsHPCC.About, onClick: () => setShowAbout(true) }
        ],
        directionalHintFixed: true
    };

    return <>
        <Stack tokens={{ padding: 9, childrenGap: 9 }} >
            <Stack horizontal disableShrink horizontalAlign="space-between">
                <Stack horizontal tokens={{ childrenGap: 18 }} >
                    <Stack.Item align="center">
                        <Link href="#/activities"><Image src="/esp/files/eclwatch/img/hpccsystems.png" /></Link>
                    </Stack.Item>
                    <Stack.Item align="center" styles={{ root: { minWidth: 240 } }}>
                        <Breadcrumb items={itemsWithHref} styles={breadCrumbStyles} />
                    </Stack.Item>
                    <Stack.Item align="center">
                        <SearchBox onSearch={newValue => { window.location.href = `#/search/${newValue.trim()}`; }} placeholder={nlsHPCC.PlaceholderFindText} styles={{ root: { minWidth: 320 } }} />
                    </Stack.Item>
                </Stack>
                <Stack horizontal tokens={{ childrenGap: 18 }} >
                    <Stack.Item align="center">
                        <Toggle
                            label="Change themes"
                            onText="Dark Mode"
                            offText="Light Mode"
                            onChange={() => {
                                setUseDarkMode(!useDarkMode);
                                const themeChangeEvent = new CustomEvent("eclwatch-theme-toggle", {
                                    detail: { dark: !useDarkMode }
                                });
                                document.dispatchEvent(themeChangeEvent);
                            }}
                        />
                    </Stack.Item>
                    <Stack.Item align="center">
                        <IconButton title={nlsHPCC.Advanced} iconProps={collapseMenuIcon} menuProps={advMenuProps} />
                    </Stack.Item>
                </Stack>
            </Stack>
        </Stack>
        <Stack horizontal styles={{ root: { background: DefaultPalette.themeLighter } }} >
        </Stack>
        <About show={showAbout} onClose={() => setShowAbout(false)} ></About>
    </>;
};
