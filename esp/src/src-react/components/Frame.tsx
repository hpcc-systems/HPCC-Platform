import * as React from "react";
import { ThemeProvider } from "@fluentui/react-theme-provider";
import { HolyGrail } from "../layouts/HolyGrail";
import { hashHistory } from "../util/history";
import { router } from "../routes";
import { darkTheme, lightTheme } from "../themes";
import { DevTitle } from "./Title";
import { DevMenu } from "./Menu";

interface DevFrameProps {
}

export const DevFrame: React.FunctionComponent<DevFrameProps> = () => {

    const [location, setLocation] = React.useState<string>("");
    const [paths, setPaths] = React.useState<string[]>([]);
    const [body, setBody] = React.useState(<h1>...loading...</h1>);
    const [showMenu] = React.useState(true);
    const [useDarkMode, setUseDarkMode] = React.useState(false);

    React.useEffect(() => {

        const unlisten = hashHistory.listen(async (location, action) => {
            setLocation(location.pathname);
            setPaths(location.pathname.split("/"));
            document.title = `ECL Watch${location.pathname.split("/").join(" | ")}`;
            setBody(await router.resolve(location));
        });

        router.resolve(hashHistory.location).then(setBody);

        return () => unlisten();
    }, []);

    return <ThemeProvider theme={useDarkMode ? darkTheme : lightTheme} style={{ height: "100%" }}>
        <HolyGrail
            header={<DevTitle paths={paths} useDarkMode={useDarkMode} setUseDarkMode={setUseDarkMode} />}
            left={showMenu ? <DevMenu location={location} /> : undefined}
            main={<>
                {body}
            </>}
        />
    </ThemeProvider >;
};
