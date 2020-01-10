import * as cookie from "dojo/cookie";
import * as xhr from "dojo/request/xhr";
import * as topic from "dojo/topic";
import * as ESPUtil from "./ESPUtil";

const espTimeoutSeconds = cookie("ESPSessionTimeoutSeconds") || 600; // 10 minuntes?
const IDLE_TIMEOUT = espTimeoutSeconds * 1000;
const SESSION_RESET_FREQ = 30 * 1000;
const idleWatcher = new ESPUtil.IdleWatcher(IDLE_TIMEOUT);
const monitorLockClick = new ESPUtil.MonitorLockClick();
const sessionIsActive = espTimeoutSeconds;

let _prevReset = Date.now();

declare const dojoConfig;

export function initSession() {
    if (sessionIsActive > -1) {
        cookie("Status", "Unlocked");
        cookie("ECLWatchUser", "true");

        idleWatcher.on("active", function () {
            resetESPTime();
        });
        idleWatcher.on("idle", function (idleCreator) {
            idleWatcher.stop();
            topic.publish("hpcc/session_management_status", {
                status: "Idle",
                idleCreator
            });
        });

        idleWatcher.start();
        monitorLockClick.unlocked();
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

function resetESPTime() {
    if (Date.now() - _prevReset > SESSION_RESET_FREQ) {
        _prevReset = Date.now();
        xhr("esp/reset_session_timeout", {
            method: "post"
        }).then(function (data) {
        });
    }
}
