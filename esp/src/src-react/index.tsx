import * as React from "react";
import * as ReactDOM from "react-dom";
import { initializeIcons } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { initSession } from "src/Session";
import { ECLWatchLogger } from "./hooks/logging";

import "css!dijit-themes/flat/flat.css";
import "css!hpcc/css/ecl.css";
import "css!hpcc/css/hpcc.css";
import "src-react-css/index.css";

ECLWatchLogger.init();
initializeIcons();

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

initSession();

import("./components/Frame").then(_ => {
    try {
        ReactDOM.render(
            <_.DevFrame />,
            document.getElementById("placeholder")
        );
    } catch (e) {
        logger.error(e);
    }
});
