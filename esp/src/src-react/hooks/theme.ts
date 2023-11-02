import * as React from "react";
import { Theme } from "@fluentui/react";
import { createDarkTheme, createLightTheme, Theme as ThemeV9 } from "@fluentui/react-components";
import { createV8Theme } from "@fluentui/react-migration-v8-v9";
import { darkTheme, lightTheme, darkThemeV9, lightThemeV9 } from "../themes";
import { getBrandTokensFromPalette } from "../util/theme/getBrandTokensFromPalette";
import { useUserStore, useGlobalStore } from "./store";
import * as Utility from "../../src/Utility";

interface UserThemeHook {
    theme: Theme;
    themeV9: ThemeV9;
    setTheme: (theme: Theme) => void;
    primaryColor: string;
    setPrimaryColor: (color: string) => void;
    hueTorsion?: number;
    setHueTorsion?: (torsion: number) => void;
    vibrancy?: number;
    setVibrancy?: (vibrancy: number) => void;
    setThemeDark: (value: "light" | "dark") => void;
    isDark: boolean;
    resetTheme: () => void;
}

const DEFAULT_HUE_TORSION = 0;
const DEFAULT_VIBRANCY = 0;

export function useUserTheme(): UserThemeHook {

    const [themeDark, setThemeDark] = useUserStore("theme", "light", true);
    const [hueTorsion, setHueTorsion] = useUserStore("theme_hueTorsion", DEFAULT_HUE_TORSION, true);
    const [vibrancy, setVibrancy] = useUserStore("theme_vibrancy", DEFAULT_VIBRANCY, true);
    const [theme, setTheme] = React.useState<Theme>();
    const [themeV9, setThemeV9] = React.useState<ThemeV9>();
    const [primaryColor, setPrimaryColor] = useUserStore("theme_primaryColor", themeV9?.colorBrandForeground1 ?? "", true);
    const [primaryColorDark, setPrimaryColorDark] = useUserStore("theme_primaryColorDark", themeV9?.colorBrandForeground1 ?? "", true);

    const resetTheme = React.useCallback(() => {
        if (themeDark === "dark") {
            setPrimaryColorDark(darkTheme.palette.themePrimary);
        } else {
            setPrimaryColor(lightTheme.palette.themePrimary);
        }
        setHueTorsion(DEFAULT_HUE_TORSION);
        setVibrancy(DEFAULT_VIBRANCY);
    }, [setHueTorsion, setPrimaryColor, setPrimaryColorDark, setVibrancy, themeDark]);

    React.useEffect(() => {
        let tokens;
        let theme9: ThemeV9;
        if (themeDark === "dark") {
            if (!primaryColorDark) return;
            tokens = getBrandTokensFromPalette(primaryColorDark, {
                hueTorsion: hueTorsion ? hueTorsion / 20 : 0,
                darkCp: vibrancy ? vibrancy / 50 : 2 / 3,
                lightCp: vibrancy ? vibrancy / 50 : 1 / 3,
            });
            theme9 = createDarkTheme(tokens);
        } else {
            if (!primaryColor) return;
            tokens = getBrandTokensFromPalette(primaryColor, {
                hueTorsion: hueTorsion ? hueTorsion / 20 : 0,
                darkCp: vibrancy ? vibrancy / 50 : 2 / 3,
                lightCp: vibrancy ? vibrancy / 50 : 1 / 3,
            });
            theme9 = createLightTheme(tokens);
        }
        if (tokens && theme9) {
            const theme8 = createV8Theme(tokens, theme9, themeDark === "dark");
            setTheme(theme8);
            setThemeV9(theme9);
        }
    }, [primaryColor, primaryColorDark, themeDark, hueTorsion, vibrancy]);

    return {
        theme: theme ?? (themeDark === "dark" ? darkTheme : lightTheme),
        themeV9: themeV9 ?? (themeDark === "dark" ? darkThemeV9 : lightThemeV9),
        setTheme: setTheme,
        primaryColor: themeDark === "dark" ? primaryColorDark : primaryColor,
        setPrimaryColor: themeDark === "dark" ? setPrimaryColorDark : setPrimaryColor,
        hueTorsion,
        setHueTorsion,
        vibrancy,
        setVibrancy,
        setThemeDark: setThemeDark,
        isDark: themeDark === "dark",
        resetTheme: resetTheme
    };
}

interface ToolbarThemeHook {
    toolbarTheme: Theme;
    toolbarThemeV9: ThemeV9;
    resetToolbarTheme: () => void;
    primaryColor: string;
    setPrimaryColor: (color: string) => void;
}

export function useToolbarTheme(): ToolbarThemeHook {

    const { isDark, themeV9 } = useUserTheme();
    const [primaryColor, setPrimaryColor, resetPrimaryColor] = useGlobalStore("HPCCPlatformWidget_Toolbar_Color", themeV9.colorBrandBackground, true);

    const [toolbarTheme, setToolbarTheme] = React.useState<Theme>();
    const [toolbarThemeV9, setToolbarThemeV9] = React.useState<ThemeV9>();

    const resetTheme = React.useCallback(() => {
        resetPrimaryColor();
    }, [resetPrimaryColor]);

    React.useEffect(() => {
        if (!primaryColor) return;
        const tokens = getBrandTokensFromPalette(primaryColor);
        const theme9: ThemeV9 = createLightTheme(tokens);
        // swap a background color to the selected color, so the banner will be the actual color picked
        theme9.colorBrandBackground2 = primaryColor;
        theme9.colorBrandForegroundLink = Utility.textColor(primaryColor);
        if (tokens && theme9) {
            const theme8 = createV8Theme(tokens, theme9, false);
            setToolbarTheme(theme8);
            setToolbarThemeV9(theme9);
        }
    }, [primaryColor]);

    React.useEffect(() => {
        if (!primaryColor) {
            resetTheme();
        }
    }, [primaryColor, resetTheme]);

    return {
        toolbarTheme: toolbarTheme ?? (isDark ? darkTheme : lightTheme),
        toolbarThemeV9: toolbarThemeV9 ?? (isDark ? darkThemeV9 : lightThemeV9),
        primaryColor,
        setPrimaryColor,
        resetToolbarTheme: resetTheme
    };
}
