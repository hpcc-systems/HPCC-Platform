/*
 *  https://fabricweb.z5.web.core.windows.net/pr-deploy-site/refs/heads/7.0/theming-designer/index.html
 *
 *  Keep in sync with themes.css
 */

import { createTheme, PartialTheme } from "@fluentui/react";
import { BrandVariants, createDarkTheme, createLightTheme } from "@fluentui/react-components";
import { createV8Theme, createV9Theme } from "@fluentui/react-migration-v8-v9";

const lightThemeOld: PartialTheme = {
    palette: {
        themePrimary: "#259ad6",
        themeLighterAlt: "#f5fbfd",
        themeLighter: "#d7edf8",
        themeLight: "#b6dff3",
        themeTertiary: "#74c0e7",
        themeSecondary: "#3ba6db",
        themeDarkAlt: "#218bc1",
        themeDark: "#1c76a3",
        themeDarker: "#145778",
        neutralLighterAlt: "#faf9f8",
        neutralLighter: "#f3f2f1",
        neutralLight: "#edebe9",
        neutralQuaternaryAlt: "#ababab",
        neutralQuaternary: "#999999",
        neutralTertiaryAlt: "#7f7f7f",
        neutralTertiary: "#6f6f6f",
        neutralSecondary: "#5f5f5f",
        neutralPrimaryAlt: "#4c4c4c",
        neutralPrimary: "#3a3a3a",
        neutralDark: "#222222",
        black: "#000000",
        white: "#ffffff",
    }
};

const darkThemeOld: PartialTheme = {
    palette: {
        themePrimary: "#ff8600",
        themeLighterAlt: "#2c2c2c",
        themeLighter: "#3a3a3a",
        themeLight: "#4d2900",
        themeTertiary: "#995200",
        themeSecondary: "#e07800",
        themeDarkAlt: "#ff9419",
        themeDark: "#ffa53d",
        themeDarker: "#ffbc70",
        neutralLighterAlt: "#323232",
        neutralLighter: "#313131",
        neutralLight: "#2f2f2f",
        neutralQuaternaryAlt: "#b0b0b0",
        neutralQuaternary: "#bababa",
        neutralTertiaryAlt: "#c3c3c3",
        neutralTertiary: "#c9c9c9",
        neutralSecondary: "#d3d3d3",
        neutralPrimaryAlt: "#dfdfdf",
        neutralPrimary: "#e1e1e1",
        neutralDark: "#f0f0f0",
        black: "#ffffff",
        white: "#222222",
    }
};

const brandWeb: BrandVariants = {
    10: "#061724",
    20: "#082338",
    30: "#0a2e4a",
    40: "#0c3b5e",
    50: "#0e4775",
    60: "#0f548c",
    70: "#115ea3",
    80: "#0f6cbd",
    90: "#2886de",
    100: "#479ef5",
    110: "#62abf5",
    120: "#77b7f7",
    130: "#96c6fa",
    140: "#b4d6fa",
    150: "#cfe4fa",
    160: "#ebf3fc",
};

const brandTeams: BrandVariants = {
    10: "#2b2b40",
    20: "#2f2f4a",
    30: "#333357",
    40: "#383966",
    50: "#3d3e78",
    60: "#444791",
    70: "#4f52b2",
    80: "#5b5fc7",
    90: "#7579eb",
    100: "#7f85f5",
    110: "#9299f7",
    120: "#aab1fa",
    130: "#b6bcfa",
    140: "#c5cbfa",
    150: "#dce0fa",
    160: "#e8ebfa",
};

const brandOffice: BrandVariants = {
    10: "#29130b",
    20: "#4d2415",
    30: "#792000",
    40: "#99482b",
    50: "#a52c00",
    60: "#c33400",
    70: "#e06a3f",
    80: "#d83b01",
    90: "#dd4f1b",
    100: "#fe7948",
    110: "#ff865a",
    120: "#ff9973",
    130: "#e8825d",
    140: "#ffb498",
    150: "#f4beaa",
    160: "#f9dcd1",
};

const brands = {
    "web": brandWeb,
    "office": brandOffice,
    "teams": brandTeams
};
const brand = brands["web"];

namespace current {
    export const lightTheme = createTheme(lightThemeOld, true);
    export const darkTheme = createTheme(darkThemeOld, true);
    export const lightThemeV9 = createV9Theme(lightTheme, createLightTheme(brand));
    export const darkThemeV9 = createV9Theme(darkTheme, createDarkTheme(brand));
}

namespace next {
    export const lightThemeV9 = createLightTheme(brand);
    export const darkThemeV9 = createDarkTheme(brand);
    export const lightTheme = createV8Theme(brand, lightThemeV9, false, current.lightTheme);
    export const darkTheme = createV8Theme(brand, darkThemeV9, true, current.darkTheme);
}

const useNext = false;
export const lightTheme = useNext ? next.lightTheme : current.lightTheme;
export const darkTheme = useNext ? next.darkTheme : current.darkTheme;
export const lightThemeV9 = useNext ? next.lightThemeV9 : current.lightThemeV9;
export const darkThemeV9 = useNext ? next.darkThemeV9 : current.darkThemeV9;
