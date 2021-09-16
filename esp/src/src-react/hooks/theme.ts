import { PartialTheme, Theme } from "@fluentui/react";
import { darkTheme, lightTheme } from "../themes";
import { useUserStore } from "./store";

export function useUserTheme(): [theme: PartialTheme | Theme, setTheme: (value: "light" | "dark") => void, isDark: boolean] {

    const [theme, setTheme] = useUserStore("theme", "light", true);

    return [theme === "dark" ? darkTheme : lightTheme, (value: "light" | "dark") => setTheme(value), theme === "dark"];
}

