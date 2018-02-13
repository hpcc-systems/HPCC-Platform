const debugServerIP = "192.168.3.22";
const debugHPCC_JS = false; //  Should never be TRUE in a PR  ---

let rewrite = [
    { from: "/esp/files/dist/*", to: "/build/dist/$1" },
    { from: "/esp/files/*", to: "/$1" },
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
