import * as React from "react";
import * as ReactDOM from "react-dom";
import { initSession } from "src/Session";

import "css!dojo-themes/flat/flat.css";
import "css!hpcc/css/ecl.css";
import "css!hpcc/css/hpcc.css";

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

ReactDOM.render(
    <h1>ECL Watch 8.0 - comming to a cloud near you in 2021</h1>,
    document.getElementById("placeholder")
);
