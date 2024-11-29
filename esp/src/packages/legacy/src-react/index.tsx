import * as React from "react";
import * as ReactDOM from "react-dom";
import { initializeIcons } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { cookieKeyValStore } from "src/KeyValStore";
import { needsRedirectV9 } from "src/Session";
import { ECLWatchLogger } from "./hooks/logging";
import { replaceUrl } from "./util/history";

import "css!dijit-themes/flat/flat.css";
import "css!hpcc/css/ecl.css";
import "css!hpcc/css/hpcc.css";
import "src-react-css/index.css";

ECLWatchLogger.init();
initializeIcons("/esp/files/dist/fluentui-fonts/");

const logger = scopedLogger("../index.tsx");

declare const dojoConfig: any;

const baseHost = "";
const hashNodes = location.hash.split("#");

dojoConfig.urlInfo = {
    baseHost,
    pathname: location.pathname,
    hash: hashNodes.length >= 2 ? hashNodes[1] : "",
    resourcePath: baseHost + "/esp/files/eclwatch",
    basePath: baseHost + "/esp/files",
    fullPath: location.origin + "/esp/files"
};
dojoConfig.disableLegacyHashing = true;

needsRedirectV9().then(async redirected => {
    if (!redirected) {
        loadUI();
    }
});

async function loadUI() {
    const authTypeResp = await fetch("/esp/getauthtype");
    const authType = await authTypeResp?.text() ?? "None";
    const userStore = cookieKeyValStore();
    const userSession = await userStore.getAll();
    if (authType.indexOf("None") < 0 && (userSession["ESPAuthenticated"] === undefined || userSession["ESPAuthenticated"] === "false")) {
        if ([...window.location.hash.matchAll(/login/gi)].length === 0) {
            replaceUrl("/login");
        }
        import("./components/forms/Login").then(_ => {
            try {
                ReactDOM.render(
                    <_.Login />,
                    document.getElementById("placeholder")
                );
                document.getElementById("loadingOverlay").remove();
            } catch (e) {
                logger.error(e);
            }
        });
    } else {
        import("./components/Frame").then(_ => {
            try {
                ReactDOM.render(
                    <_.Frame />,
                    document.getElementById("placeholder")
                );
                document.getElementById("loadingOverlay").remove();
            } catch (e) {
                logger.error(e);
            }
        });
    }
}
