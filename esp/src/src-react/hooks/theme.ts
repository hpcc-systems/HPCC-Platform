import * as React from "react";
import { BaseSlots, createTheme, Theme, ThemeGenerator, themeRulesStandardCreator } from "@fluentui/react";
import { Theme as ThemeV9 } from "@fluentui/react-components";
import { darkTheme, lightTheme, darkThemeV9, lightThemeV9 } from "../themes";
import { useUserStore } from "./store";

interface UserThemeHook {
    theme: Theme;
    themeV9: ThemeV9;
    setTheme: (theme: Theme) => void;
    primaryColor: string;
    setPrimaryColor: (color: string) => void;
    textColor: string;
    setTextColor: (color: string) => void;
    backgroundColor: string;
    setBackgroundColor: (color: string) => void;
    setThemeDark: (value: "light" | "dark") => void;
    isDark: boolean;
    resetTheme: () => void;
}

export function useUserTheme(): UserThemeHook {

    const [themeDark, setThemeDark] = useUserStore("theme", "light", true);
    const [primaryColor, setPrimaryColor] = useUserStore("theme_primaryColor", lightTheme.palette.themePrimary, true);
    const [textColor, setTextColor] = useUserStore("theme_textColor", lightTheme.palette.neutralPrimary, true);
    const [backgroundColor, setBackgroundColor] = useUserStore("theme_backgroundColor", lightTheme.palette.white, true);
    const [primaryColorDark, setPrimaryColorDark] = useUserStore("theme_primaryColorDark", darkTheme.palette.themePrimary, true);
    const [textColorDark, setTextColorDark] = useUserStore("theme_textColorDark", darkTheme.palette.neutralPrimary, true);
    const [backgroundColorDark, setBackgroundColorDark] = useUserStore("theme_backgroundColorDark", darkTheme.palette.white, true);

    const generateTheme = React.useCallback(() => {
        const themeRules = themeRulesStandardCreator();

        ThemeGenerator.insureSlots(themeRules, themeDark === "dark");
        ThemeGenerator.setSlot(themeRules[BaseSlots[BaseSlots.primaryColor]], themeDark === "dark" ? primaryColorDark : primaryColor, themeDark === "dark", true, true);
        ThemeGenerator.setSlot(themeRules[BaseSlots[BaseSlots.foregroundColor]], themeDark === "dark" ? textColorDark : textColor, themeDark === "dark", true, true);
        ThemeGenerator.setSlot(themeRules[BaseSlots[BaseSlots.backgroundColor]], themeDark === "dark" ? backgroundColorDark : backgroundColor, themeDark === "dark", true, true);

        const themeAsJson: {
            [key: string]: string;
        } = ThemeGenerator.getThemeAsJson(themeRules);

        const finalTheme = createTheme({
            ...{ palette: themeAsJson },
            isInverted: themeDark === "dark",
        });
        return finalTheme;
    }, [primaryColor, primaryColorDark, textColor, textColorDark, backgroundColor, backgroundColorDark, themeDark]);

    const [theme, setTheme] = React.useState<Theme>(generateTheme());

    const resetTheme = React.useCallback(() => {
        if (themeDark === "dark") {
            setPrimaryColorDark(darkTheme.palette.themePrimary);
            setTextColorDark(darkTheme.palette.neutralPrimary);
            setBackgroundColorDark(darkTheme.palette.white);
        } else {
            setPrimaryColor(lightTheme.palette.themePrimary);
            setTextColor(lightTheme.palette.neutralPrimary);
            setBackgroundColor(lightTheme.palette.white);
        }
    }, [setBackgroundColor, setBackgroundColorDark, setPrimaryColor, setPrimaryColorDark, setTextColor, setTextColorDark, themeDark]);

    React.useEffect(() => {
        setTheme(generateTheme());
    }, [generateTheme]);

    React.useEffect(() => {
        if (!primaryColor) {
            resetTheme();
        }
    }, [primaryColor, resetTheme]);

    return {
        theme: theme,
        themeV9: themeDark === "dark" ? darkThemeV9 : lightThemeV9,
        setTheme: setTheme,
        primaryColor: themeDark === "dark" ? primaryColorDark : primaryColor,
        setPrimaryColor: themeDark === "dark" ? setPrimaryColorDark : setPrimaryColor,
        textColor: themeDark === "dark" ? textColorDark : textColor,
        setTextColor: themeDark === "dark" ? setTextColorDark : setTextColor,
        backgroundColor: themeDark === "dark" ? backgroundColorDark : backgroundColor,
        setBackgroundColor: themeDark === "dark" ? setBackgroundColorDark : setBackgroundColor,
        setThemeDark: setThemeDark,
        isDark: themeDark === "dark",
        resetTheme: resetTheme
    };
}

