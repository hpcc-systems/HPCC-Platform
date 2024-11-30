const fs = require("fs");

let ip = "192.168.99.103";
if (fs.existsSync("./lws.target.txt")) {
    ip = fs.readFileSync("./lws.target.txt").toString().replace("\r\n", "\n").split("\n")[0];
}

let protocol = "http";
let tartgetParts = ip.split("://");
if (tartgetParts.length > 1) {
    protocol = tartgetParts[0];
    ip = tartgetParts[1];
}

let port = 8010;
tartgetParts = ip.split(":");
if (tartgetParts.length > 1) {
    ip = tartgetParts[0];
    port = tartgetParts[1];
}
console.log("Protocol:  " + protocol);
console.log("IP:  " + ip);
console.log("Port:  " + port);

let rewrite = [
    { from: "/esp/titlebar(.*)", to: protocol + "://" + ip + ":" + port + "/esp/titlebar$1" },
    { from: "/esp/login", to: protocol + "://" + ip + ":" + port + "/esp/login" },
    { from: "/esp/logout", to: protocol + "://" + ip + ":" + port + "/esp/logout" },
    { from: "/esp/lock", to: protocol + "://" + ip + ":" + port + "/esp/lock" },
    { from: "/esp/reset_session_timeout", to: protocol + "://" + ip + ":" + port + "/esp/reset_session_timeout" },
    { from: "/esp/getauthtype", to: protocol + "://" + ip + ":" + port + "/esp/getauthtype" },
    { from: "/esp/files/esp/getauthtype", to: protocol + "://" + ip + ":" + port + "/esp/getauthtype" },
    { from: "/esp/files/esp/lock", to: protocol + "://" + ip + ":" + port + "/esp/lock" },
    { from: "/esp/unlock.json", to: protocol + "://" + ip + ":" + port + "/esp/unlock.json" },
    { from: "/esp/files/esp/logout", to: protocol + "://" + ip + ":" + port + "/esp/logout" },
    { from: "/esp/files/esp/reset_session_timeout", to: protocol + "://" + ip + ":" + port + "/esp/reset_session_timeout" },
    { from: "/esp/files/node_modules/@hpcc-js/(.*)/dist/index.min.js", to: "/node_modules/@hpcc-js/$1/dist/index.js" },
    { from: "/esp/files/dist/(.*)", to: "/build/dist/$1" },
    { from: "/esp/files/img/(.*)", to: "build/esp/files/img/$1" },
    { from: "/esp/files/(.*/*.css)", to: "/build/esp/files/$1" },
    { from: "/esp/files/(.*)", to: "/build/$1" },
    { from: "/main", to: protocol + "://" + ip + ":" + port + "/main" },
    { from: "/FileSpray/(.*)", to: protocol + "://" + ip + ":" + port + "/FileSpray/$1" },
    { from: "/WsCloud/(.*)", to: protocol + "://" + ip + ":" + port + "/WsCloud/$1" },
    { from: "/WSDali/(.*)", to: protocol + "://" + ip + ":" + port + "/WSDali/$1" },
    { from: "/WsDfu/(.*)", to: protocol + "://" + ip + ":" + port + "/WsDfu/$1" },
    { from: "/WsDfuXRef/(.*)", to: protocol + "://" + ip + ":" + port + "/WsDfuXRef/$1" },
    { from: "/WsECL/(.*)", to: protocol + "://" + ip + ":8002/WsECL/$1" },
    { from: "/WsESDLConfig/(.*)", to: protocol + "://" + ip + ":" + port + "/WsESDLConfig/$1" },
    { from: "/WsFileIO/(.*)", to: protocol + "://" + ip + ":" + port + "/WsFileIO/$1" },
    { from: "/WsPackageProcess/(.*)", to: protocol + "://" + ip + ":" + port + "/WsPackageProcess/$1" },
    { from: "/WsSMC/(.*)", to: protocol + "://" + ip + ":" + port + "/WsSMC/$1" },
    { from: "/WsStore/(.*)", to: protocol + "://" + ip + ":" + port + "/WsStore/$1" },
    { from: "/WsTopology/(.*)", to: protocol + "://" + ip + ":" + port + "/WsTopology/$1" },
    { from: "/WsWorkunits/(.*)", to: protocol + "://" + ip + ":" + port + "/WsWorkunits/$1" },
    { from: "/ws_access/(.*)", to: protocol + "://" + ip + ":" + port + "/ws_access/$1" },
    { from: "/ws_account/(.*)", to: protocol + "://" + ip + ":" + port + "/ws_account/$1" },
    { from: "/ws_codesign/(.*)", to: protocol + "://" + ip + ":" + port + "/ws_codesign/$1" },
    { from: "/ws_elk/(.*)", to: protocol + "://" + ip + ":" + port + "/ws_elk/$1" },
    { from: "/ws_esdlconfig/(.*)", to: protocol + "://" + ip + ":" + port + "/ws_esdlconfig/$1" },
    { from: "/ws_logaccess/(.*)", to: protocol + "://" + ip + ":" + port + "/ws_logaccess/$1" },
    { from: "/ws_machine/(.*)", to: protocol + "://" + ip + ":" + port + "/ws_machine/$1" },
    { from: "/WsResources/(.*)", to: protocol + "://" + ip + ":" + port + "/WsResources/$1" },
    { from: "/ws_store/(.*)", to: protocol + "://" + ip + ":" + port + "/ws_store/$1" },
    { from: "/(.*)", to: "/$1" }
];

module.exports = {
    port: 8080,
    rewrite: rewrite,
    stack: ['lws-basic-auth', 'lws-request-monitor', 'lws-log', 'lws-cors', 'lws-json', 'lws-compress', 'lws-rewrite', 'lws-blacklist', 'lws-conditional-get', 'lws-mime', 'lws-range', 'lws-spa', 'lws-static', 'lws-index']
};