import { Theme as ThemeV9 } from "@fluentui/react-components";
import { userKeyValStore } from "src/KeyValStore";
import { darkThemeV9, lightThemeV9 } from "../themes";
import { useUserStore } from "./store";

const THEME = "theme";
const NAV_WIDE = "navWide";

export function resetNavWide() {
    const store = userKeyValStore();
    return store?.delete(NAV_WIDE);
}

export function resetTheme() {
    const store = userKeyValStore();
    return store?.delete(THEME);
}

export function useNavWide(): { navWide: boolean, setNavWide: (value: boolean) => void } {

    const [navWide, setNavWide] = useUserStore(NAV_WIDE, false);

    return { navWide, setNavWide };
}

export function useLightTheme(): { themeV9: ThemeV9 } {

    return {
        themeV9: lightThemeV9,
    };
}

export function useUserTheme(): { themeV9: ThemeV9, setTheme: (value: "light" | "dark") => void, isDark: boolean } {

    const [theme, setTheme] = useUserStore(THEME, "light", true);

    return {
        themeV9: theme === "dark" ? darkThemeV9 : lightThemeV9,
        setTheme: (value: "light" | "dark") => setTheme(value),
        isDark: theme === "dark"
    };
}
