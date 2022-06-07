import * as React from "react";
import { ThemeProvider } from "@fluentui/react";
import { select as d3Select } from "@hpcc-js/common";
import { scopedLogger } from "@hpcc-js/util";
import { useUserTheme } from "../hooks/theme";
import { HolyGrail } from "../layouts/HolyGrail";
import { hashHistory } from "../util/history";
import { router } from "../routes";
import { DevTitle } from "./Title";
import { MainNavigation, SubNavigation } from "./Menu";
import { CookieConsent } from "./forms/CookieConsent";
import { userKeyValStore } from "../../src/KeyValStore";
import { useGlobalStore } from "../hooks/store";

const logger = scopedLogger("../components/Frame.tsx");

interface FrameProps {
}

export const Frame: React.FunctionComponent<FrameProps> = () => {

    const [showCookieConsent, setShowCookieConsent] = React.useState(false);
    const [locationPathname, setLocationPathname] = React.useState<string>(window.location.hash.split("#").join(""));
    const [body, setBody] = React.useState(<h1>...loading...</h1>);
    const [theme, , isDark] = useUserTheme();
    const [showEnvironmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Active", false, true);
    const [environmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Text", "", true);

    React.useEffect(() => {

        const unlisten = hashHistory.listen(async (location, action) => {
            logger.debug(location.pathname);
            setLocationPathname(location.pathname);
            setBody(await router.resolve(location));
        });

        router.resolve(hashHistory.location).then(setBody);

        userKeyValStore().get("user_cookie_consent")
            .then((resp) => {
                setShowCookieConsent(resp === "1");
            })
            ;

        return () => unlisten();
    }, []);

    React.useEffect(() => {
        document.title = `${showEnvironmentTitle && environmentTitle.length ? environmentTitle : "ECL Watch - 9"}${locationPathname.split("/").join(" | ")}`;
    }, [environmentTitle, locationPathname, showEnvironmentTitle]);

    React.useEffect(() => {
        d3Select("body")
            .classed("flat-dark", isDark)
            ;
    }, [isDark]);

    return <ThemeProvider theme={theme} style={{ height: "100%" }}>
        <HolyGrail
            header={<DevTitle />}
            left={<MainNavigation hashPath={locationPathname} />}
            main={<HolyGrail
                header={<SubNavigation hashPath={locationPathname} />}
                main={body}
            />}
        />
        <CookieConsent showCookieConsent={showCookieConsent} onApply={(n: boolean) => {
            userKeyValStore().set("user_cookie_consent", n ? "1" : "0");
        }} />
    </ThemeProvider >;
};
