import * as React from "react";
import { Breadcrumb, DefaultPalette, FontSizes, IBreadcrumbItem, IBreadcrumbStyleProps, IBreadcrumbStyles, Image, IStyleFunctionOrObject, Link, SearchBox, Stack, Toggle } from "@fluentui/react";

import nlsHPCC from "src/nlsHPCC";

const breadCrumbStyles: IStyleFunctionOrObject<IBreadcrumbStyleProps, IBreadcrumbStyles> = {
    itemLink: { fontSize: FontSizes.size10, lineHeight: 14, paddingLeft: 2, paddingRight: 2 },
};

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

    let fullPath = "#";
    const itemsWithHref = [{ text: "HOME", key: "home", href: "#/" },
    ...paths.filter(path => !!path).map((path, idx) => {
        const retVal: IBreadcrumbItem = { text: path.toUpperCase(), key: "" + idx, href: `${fullPath}/${path}` };
        fullPath = `${fullPath}/${path}`;
        return retVal;
    })];

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
                        <Link href="/esp/files/stub.htm">Legacy ECL Watch</Link>
                        <Toggle
                            label="Change themes"
                            onText="Dark Mode"
                            offText="Light Mode"
                            onChange={() => setUseDarkMode(!useDarkMode)}
                        />
                    </Stack.Item>
                </Stack>
            </Stack>
        </Stack>
        <Stack horizontal styles={{ root: { background: DefaultPalette.themeLighter } }} >
        </Stack>
    </>;
};
