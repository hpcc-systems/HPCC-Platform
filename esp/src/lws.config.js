const CLUSTER_MV = "192.168.99.103";
const CLUSTER_GJS = "192.168.3.22";
const CLUSTER_160 = "10.173.160.101";
const debugServerIP = CLUSTER_MV ;
const debugHPCC_JS = false; //  Should never be TRUE in a PR  ---

let rewrite = [
    { from: "/esp/files/Login.html", to: "http://" + debugServerIP + ":8010/esp/files/Login.html" },
    { from: "/esp/files/GetUserName.html", to: "http://" + debugServerIP + ":8010/esp/files/GetUserName.html" },
    { from: "/esp/login", to: "http://" + debugServerIP + ":8010/esp/login" },
    { from: "/esp/logout", to: "http://" + debugServerIP + ":8010/esp/logout" },
    { from: "/esp/lock", to: "http://" + debugServerIP + ":8010/esp/lock" },
    { from: "/esp/reset_session_timeout", to: "http://" + debugServerIP + ":8010/esp/reset_session_timeout" },
    { from: "/esp/getauthtype", to: "http://" + debugServerIP + ":8010/esp/getauthtype" },
    { from: "/esp/files/esp/getauthtype", to: "http://" + debugServerIP + ":8010/esp/getauthtype" },
    { from: "/esp/files/esp/lock", to: "http://" + debugServerIP + ":8010/esp/lock" },
    { from: "/esp/unlock.json", to: "http://" + debugServerIP + ":8010/esp/unlock.json" },
    { from: "/esp/files/esp/logout", to: "http://" + debugServerIP + ":8010/esp/logout" },
    { from: "/ws_elk/*", to: "http://" + debugServerIP + ":8010/ws_elk/$1" },
    { from: "/esp/files/esp/reset_session_timeout", to: "http://" + debugServerIP + ":8010/esp/reset_session_timeout" },
    { from: "/esp/files/dist/*", to: "/build/dist/$1" },
    { from: "/esp/files/*", to: "/$1" },
    { from: "/ws_elk/*", to: "http://" + debugServerIP + ":8010/ws_elk/$1" },
    { from: "/FileSpray/*", to: "http://" + debugServerIP + ":8010/FileSpray/$1" },
    { from: "/WsWorkunits/*", to: "http://" + debugServerIP + ":8010/WsWorkunits/$1" },
    { from: "/main", to: "http://" + debugServerIP + ":8010/main" },
    { from: "/WsECL/*", to: "http://" + debugServerIP + ":8002/WsECL/$1" },
    { from: "/WsTopology/*", to: "http://" + debugServerIP + ":8010/WsTopology/$1" },
    { from: "/WsSMC/*", to: "http://" + debugServerIP + ":8010/WsSMC/$1" },
    { from: "/ws_machine/*", to: "http://" + debugServerIP + ":8010/ws_machine/$1" },
    { from: "/ws_account/*", to: "http://" + debugServerIP + ":8010/ws_account/$1" },
    { from: "/ws_access/*", to: "http://" + debugServerIP + ":8010/ws_access/$1" },
    { from: "/WsESDLConfig/*", to: "http://" + debugServerIP + ":8010/WsESDLConfig/$1" },
    { from: "/WsDfu/*", to: "http://" + debugServerIP + ":8010/WsDfu/$1" },
    { from: "/WsDFUXRef/*", to: "http://" + debugServerIP + ":8010/WsDFUXRef/$1" },
    { from: "/WsPackageProcess/*", to: "http://" + debugServerIP + ":8010/WsPackageProcess/$1" },
    { from: "/*", to: "/$1" }
];

if (debugHPCC_JS) {
    rewrite = [
        { from: "/esp/files/node_modules/@hpcc-js/*/dist/index.min.js", to: "/node_modules/@hpcc-js/$1/dist/index.js" }
    ].concat(rewrite);
}

module.exports = {
    port: 8080,
    rewrite: rewrite
}
