
import * as React from "react";
import { ReactNode } from "react";
import { ThemeProvider } from "@fluentui/react";
import { FluentProvider, makeStyles } from "@fluentui/react-components";
import { useLightTheme } from "../../src-react/hooks/theme";

const useStyles = makeStyles({
    root: {
        height: "100%"
    }
});

export interface LightThemedProps {
    children?: ReactNode;
}

export const LightThemed = ({ children }: LightThemedProps) => {

    const { theme, themeV9 } = useLightTheme();
    const styles = useStyles();

    return <FluentProvider theme={themeV9} className={styles.root}>
        <ThemeProvider theme={theme} className={styles.root}>
            {children}
        </ThemeProvider>
    </FluentProvider>;
};

