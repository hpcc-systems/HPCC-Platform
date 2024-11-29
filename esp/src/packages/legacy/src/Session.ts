import * as cookie from "dojo/cookie";
import * as xhr from "dojo/request/xhr";
import * as topic from "dojo/topic";
import { format as d3Format } from "@hpcc-js/common";
import { SMCService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { cookieKeyValStore, sessionKeyValStore, userKeyValStore } from "src/KeyValStore";
import { singletonDebounce } from "../src-react/util/throttle";
import { parseSearch, replaceUrl } from "../src-react/util/history";
import { ModernMode } from "./BuildInfo";
import * as ESPUtil from "./ESPUtil";

const logger = scopedLogger("src/Session.ts");

const espTimeoutSeconds = cookie("ESPSessionTimeoutSeconds") || 600; // 10 minuntes?
const IDLE_TIMEOUT = espTimeoutSeconds * 1000;
const SESSION_RESET_FREQ = 30 * 1000;
const idleWatcher = new ESPUtil.IdleWatcher(IDLE_TIMEOUT);
const sessionIsActive = espTimeoutSeconds;

let _prevReset = Date.now();

declare const dojoConfig;

const cookieStore = cookieKeyValStore();
const sessionStore = sessionKeyValStore();
const userStore = userKeyValStore();

export async function fetchModernMode(): Promise<string> {
    return Promise.all([
        sessionStore.get(ModernMode),
        userStore.getEx(ModernMode, { defaultValue: String(true) })
    ]).then(([sessionModernMode, userModernMode]) => {
        return sessionModernMode ?? userModernMode;
    });
}

export async function resetModernMode() {
    await sessionStore.delete(ModernMode);
    await userStore.delete(ModernMode);
}

export async function resetCookies() {
    await cookieStore.deleteAll(true);
}

const isV5DirectURL = () => !!parseSearch(window.location.search)?.["Widget"];
const isV9DirectURL = () => window.location.hash && window.location.hash.indexOf("#/stub/") !== 0;

export async function needsRedirectV5(): Promise<boolean> {
    if (isV9DirectURL()) {
        window.location.replace(`/esp/files/index.html${window.location.hash}`);
        return true;
    }
    if (window.location.pathname.indexOf("/esp/files/stub.htm") < 0) {
        window.location.replace(`/esp/files/stub.htm${window.location.search}${window.location.hash}`);
        return true;
    }
    if (isV5DirectURL()) {
        return false;
    }

    const v9Mode = await fetchModernMode() === String(true);
    if (v9Mode) {
        const params = parseSearch(window.location.search);
        if (params?.["hpccWidget"] !== "IFrameWidget") {
            switch (params?.["hpccWidget"]) {
                case "WUDetailsWidget":
                    window.location.replace(`/esp/files/index.html#/workunits/${params.Wuid}`);
                    break;
                case "GraphsWUWidget":
                    window.location.replace(`/esp/files/index.html#/workunits/${params.Wuid}/metrics`);
                    break;
                case "TopologyWidget":
                case "DiskUsageWidget":
                case "TargetClustersQueryWidget":
                case "ClusterProcessesQueryWidget":
                case "SystemServersQueryWidget":
                case "LogWidget":
                    return false;
                default:
                    window.location.replace("/esp/files/index.html");
            }
            return true;
        }
    }
    return false;
}

export async function needsRedirectV9(): Promise<boolean> {
    if (isV5DirectURL()) {
        window.location.replace(`/esp/files/stub.htm${window.location.search}`);
        return true;
    }
    if (isV9DirectURL()) {
        return false;
    }

    const v5Mode = await fetchModernMode() === String(false);
    if (v5Mode) {
        window.location.replace(`/esp/files/stub.htm${window.location.search}`);
        return true;
    }
    return false;
}

const smc = new SMCService({ baseUrl: "" });

export type BuildInfo = { [key: string]: string };

export async function getBuildInfo(): Promise<BuildInfo> {
    const getBuildInfo = singletonDebounce(smc, "GetBuildInfo", 60);
    return getBuildInfo({}).then(response => {
        const buildInfo = {};
        response?.BuildInfo?.NamedValue?.forEach(row => {
            buildInfo[row.Name] = row.Value;
        });
        return buildInfo;
    }).catch(e => {
        logger.error(e);
        return {};
    });
}

dojoConfig.isContainer = false;
dojoConfig.currencyCode = "";
getBuildInfo().then(info => {
    dojoConfig.isContainer = info["CONTAINERIZED"] === "ON";
    dojoConfig.currencyCode = info["currencyCode"] ?? "";
});

export const formatTwoDigits = d3Format(",.2f");
const formatSixDigits = d3Format(",.6f");
export function formatCost(value): string {
    if (isNaN(value)) {
        logger.debug(`formatCost called for a nullish value: ${value}`);
        return "";
    }
    const _number = typeof value === "string" ? Number(value) : value;
    const format = (_number > 0 && _number < 1) ? formatSixDigits : formatTwoDigits;
    return `${format(_number)} (${dojoConfig?.currencyCode || "USD"})`;
}

export function initSession() {
    if (sessionIsActive > -1) {

        const redirectUrl = window.localStorage.getItem("redirectAfterLogin") ?? "";
        if (redirectUrl) {
            window.localStorage.removeItem("redirectAfterLogin");
            replaceUrl(redirectUrl);
        }
        idleWatcher.on("active", function () {
            resetESPTime();
        });
        idleWatcher.on("idle", function (idleCreator) {
            idleWatcher.stop();
            fetch("/esp/lock", {
                method: "post"
            }).then(() => {
                topic.publish("hpcc/session_management_status", {
                    status: "Idle",
                    idleCreator
                });
            });
        });

        idleWatcher.start();
    } else if (cookie("ECLWatchUser")) {
        window.location.replace(dojoConfig.urlInfo.basePath + "/Login.html");
    }
}

export function lock() {
    idleWatcher.stop();
}

export function unlock() {
    idleWatcher.start();
}

export function fireIdle() {
    idleWatcher.fireIdle();
}

async function resetESPTime() {
    const userSession = cookieStore.getAll();
    if (!userSession || !userSession["ECLWatchUser"] || !userSession["Status"] || userSession["Status"] === "Locked") return;
    if (Date.now() - _prevReset > SESSION_RESET_FREQ) {
        _prevReset = Date.now();
        xhr("esp/reset_session_timeout", {
            method: "post"
        }).then(function (data) {
        });
    }
}
