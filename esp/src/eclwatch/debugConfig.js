var DEBUG_HPCC_JS = false;
if (DEBUG_HPCC_JS) {
    for (var key in dojoConfig.paths) {
        if (key.indexOf("@hpcc-js/") === 0) {
            dojoConfig.paths[key] = dojoConfig.paths[key].replace("/node_modules/@hpcc-js/", "/hpcc-js/packages/");
        }
    }
}
