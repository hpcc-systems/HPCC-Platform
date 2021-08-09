import * as React from "react";
import { ThemeProvider } from "@fluentui/react";
import { select as d3Select } from "@hpcc-js/common";
import { scopedLogger } from "@hpcc-js/util";
import { HolyGrail } from "../layouts/HolyGrail";
import { hashHistory } from "../util/history";
import { router } from "../routes";
import { darkTheme, lightTheme } from "../themes";
import { DevTitle } from "./Title";
import { MainNavigation, SubNavigation } from "./Menu";

const logger = scopedLogger("../components/Frame.tsx");

interface DevFrameProps {
}

export const DevFrame: React.FunctionComponent<DevFrameProps> = () => {

    const [location, setLocation] = React.useState<string>(window.location.hash.split("#").join(""));
    const [body, setBody] = React.useState(<h1>...loading...</h1>);
    const [useDarkMode, setUseDarkMode] = React.useState(false);

    React.useEffect(() => {

        const unlisten = hashHistory.listen(async (location, action) => {
            logger.debug(location.pathname);
            setLocation(location.pathname);
            document.title = `ECL Watch${location.pathname.split("/").join(" | ")}`;
            setBody(await router.resolve(location));
        });

        router.resolve(hashHistory.location).then(setBody);

        return () => unlisten();
    }, []);

    React.useEffect(() => {
        d3Select("body")
            .classed("flat-dark", useDarkMode)
            ;
    }, [useDarkMode]);

    return <ThemeProvider theme={useDarkMode ? darkTheme : lightTheme} style={{ height: "100%" }}>
        <HolyGrail
            header={<DevTitle useDarkMode={useDarkMode} setUseDarkMode={setUseDarkMode} />}
            left={<MainNavigation hashPath={location} useDarkMode={useDarkMode} setUseDarkMode={setUseDarkMode} />}
            main={<HolyGrail
                header={<SubNavigation hashPath={location} />}
                main={body}
            />}
        />
    </ThemeProvider >;
};
