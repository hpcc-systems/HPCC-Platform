import * as React from "react";
import * as topic from "dojo/topic";
import { ThemeProvider } from "@fluentui/react";
import { FluentProvider } from "@fluentui/react-components";
import { select as d3Select } from "@hpcc-js/common";
import { scopedLogger } from "@hpcc-js/util";
import { HolyGrail } from "../layouts/HolyGrail";
import { hashHistory } from "../util/history";
import { router } from "../routes";
import { DevTitle } from "./Title";
import { MainNavigation, SubNavigation } from "./Menu";
import { CookieConsent } from "./forms/CookieConsent";
import { userKeyValStore } from "src/KeyValStore";
import { fireIdle, initSession, lock, unlock } from "src/Session";
import { useGlobalStore } from "../hooks/store";
import { useUserTheme } from "../hooks/theme";
import { useGlobalWorkunitNotes } from "../hooks/workunit";
import { useUserSession } from "../hooks/user";

const logger = scopedLogger("../components/Frame.tsx");
const envLogger = scopedLogger("environment");

const USER_COOKIE_CONSENT = "user_cookie_consent";

export function resetCookieConsent() {
    const store = userKeyValStore();
    return store.delete(USER_COOKIE_CONSENT);
}

interface FrameProps {
}

export const Frame: React.FunctionComponent<FrameProps> = () => {

    const [showCookieConsent, setShowCookieConsent] = React.useState(false);
    const { userSession, setUserSession } = useUserSession();
    const [locationPathname, setLocationPathname] = React.useState<string>(window.location.hash.split("#").join(""));
    const [body, setBody] = React.useState(<h1>...loading...</h1>);
    const { theme, themeV9, isDark } = useUserTheme();
    const [showEnvironmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Active", false, true);
    const [environmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Text", "", true);

    const [globalWUNotes] = useGlobalWorkunitNotes();

    React.useEffect(() => {
        globalWUNotes.forEach(note => {
            switch (note.Severity) {
                case "warning":
                    envLogger.warning(note.Message);
                    break;
                case "error":
                    envLogger.error(note.Message);
                    break;
                case "info":
                default:
                    envLogger.info(note.Message);
                    break;

            }
        });
    }, [globalWUNotes]);

    React.useEffect(() => {
        initSession();

        topic.subscribe("hpcc/session_management_status", function (publishedMessage) {
            if (publishedMessage.status === "Unlocked") {
                unlock();
            } else if (publishedMessage.status === "Locked") {
                lock();
            } else if (publishedMessage.status === "DoIdle") {
                fireIdle();
            } else if (publishedMessage.status === "Idle") {
                window.localStorage.setItem("pageOnLock", window.location.hash.substring(1));
                setUserSession({ ...userSession });
                window.location.reload();
            }
        });
    }, [setUserSession, userSession]);

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
        document.title = `${(showEnvironmentTitle && environmentTitle) ? environmentTitle : "ECL Watch "}${locationPathname.split("/").join(" | ")}`;
    }, [environmentTitle, locationPathname, showEnvironmentTitle]);

    React.useEffect(() => {
        d3Select("body")
            .classed("flat-dark", isDark)
            ;
    }, [isDark]);

    return <FluentProvider theme={themeV9} style={{ height: "100%" }}>
        <ThemeProvider theme={theme} style={{ height: "100%" }}>
            <HolyGrail
                header={<DevTitle />}
                left={<MainNavigation hashPath={locationPathname} />}
                main={<HolyGrail
                    header={<SubNavigation hashPath={locationPathname} />}
                    main={body}
                />}
            />
            <CookieConsent showCookieConsent={showCookieConsent} onApply={(n: boolean) => {
                userKeyValStore().set(USER_COOKIE_CONSENT, n ? "1" : "0");
            }} />
        </ThemeProvider >
    </FluentProvider >;
};
