
import * as React from "react";
import { ReactNode } from "react";
import { ThemeProvider } from "@fluentui/react";
import { FluentProvider, makeStyles } from "@fluentui/react-components";
import { useUserTheme, useLightTheme } from "../../src-react/hooks/theme";

const useStyles = makeStyles({
    root: {
        height: "100%"
    }
});

export interface ThemedProps {
    children?: ReactNode;
}

export const Themed = ({ children }: ThemedProps) => {

    const { theme, themeV9 } = useUserTheme();
    const styles = useStyles();

    return <FluentProvider theme={themeV9} className={styles.root}>
        <ThemeProvider theme={theme} className={styles.root}>
            {children}
        </ThemeProvider>
    </FluentProvider>;
};

export const LightThemed = ({ children }: ThemedProps) => {

    const { theme, themeV9 } = useLightTheme();
    const styles = useStyles();

    return <FluentProvider theme={themeV9} className={styles.root}>
        <ThemeProvider theme={theme} className={styles.root}>
            {children}
        </ThemeProvider>
    </FluentProvider>;
};

