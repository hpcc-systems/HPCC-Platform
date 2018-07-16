const debugServerIP = "192.168.99.103";
const debugHPCC_JS = false; //  Should never be TRUE in a PR  ---

let rewrite = [
    { from: "/esp/files/Login.html", to: "http://" + debugServerIP + ":8010/esp/files/Login.html" },
    { from: "/esp/files/GetUserName.html", to: "http://" + debugServerIP + ":8010/esp/files/GetUserName.html" },
    { from: "/esp/login", to: "http://" + debugServerIP + ":8010/esp/login" },
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
        { from: "/esp/files/node_modules/@hpcc-js/*", to: "/hpcc-js/packages/$1" }
    ].concat(rewrite);
}

module.exports = {
    port: 8080,
    rewrite: rewrite
}