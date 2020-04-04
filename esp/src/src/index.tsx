import * as React from "react";
import * as ReactDOM from "react-dom";
import { MuiThemeProvider, createMuiTheme } from "@material-ui/core";
import CssBaseline from "@material-ui/core/CssBaseline";
import deepOrange from "@material-ui/core/colors/deepOrange";
import { Frame } from "./react/frame";
import { initSession } from "./Session";

import "css!hpcc/css/ecl.css";
import "css!dojo-themes/flat/flat.css";
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

initSession();

const theme = createMuiTheme({
    palette: {
        primary: {
            light: "#8561c5",
            main: "#673ab7",
            dark: "#482880",
            contrastText: "#fff"
        },
        secondary: deepOrange
    }
});

ReactDOM.render(
    <MuiThemeProvider theme={theme}>
        <CssBaseline>
            <Frame />
        </CssBaseline>
    </MuiThemeProvider>,
    document.getElementById("app")
);
