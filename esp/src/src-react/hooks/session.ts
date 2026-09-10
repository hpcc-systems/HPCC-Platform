import * as React from "react";
import { scopedLogger } from "@hpcc-js/util";
import * as Utility from "src/Utility";
import { replaceUrl } from "../util/history";
import { IdleWatcher } from "../util/idleWatcher";

const logger = scopedLogger("src-react/hooks/session.ts");

const SESSION_RESET_FREQ = 30 * 1000;
const DEFAULT_TIMEOUT_SECONDS = 600;
const REDIRECT_AFTER_LOGIN = "redirectAfterLogin";
const CHANNEL_NAME = "eclwatch_session";

declare const dojoConfig;

type BroadcastMessage = { type: "expired" | "locked" | "authenticated" };

function cookies(): Record<string, string> {
    return Utility.parseCookies();
}

function isAuthenticated(): boolean {
    return cookies()["ESPAuthenticated"] === "true";
}

//  True only when the environment uses session-based auth (Mixed / PerSessionOnly).  When false
//  (no-auth, per-request or username-only) the controller stays inert and never redirects.
function sessionAuthEnabled(): boolean {
    return cookies()["ESPSessionState"] === "true";
}

function idleTimeoutMs(): number {
    const seconds = Number(cookies()["ESPSessionTimeoutSeconds"]) || DEFAULT_TIMEOUT_SECONDS;
    return seconds * 1000;
}

//  Full-page navigation to the server login page - leaves the SPA so all polling stops and
//  the server auth flow (redirectAfterLogin) handles returning to the current page.
function loginUrl(): string {
    return `${dojoConfig?.urlInfo?.basePath ?? "/esp/files"}/Login.html`;
}

export function redirectToLogin(preservePath: boolean = true): void {
    const target = loginUrl();
    window.location.href = preservePath ? `${target}${window.location.hash}` : target;
}

//  React-owned session controller.  Shares only cookies with the Dojo UI - it does not use
//  the Dojo IdleWatcher, ESPRequest or the "hpcc/session_management_status" topic.
class SessionController {

    private _watcher = new IdleWatcher(idleTimeoutMs());
    private _prevReset = 0;
    private _locked = false;
    private _started = false;
    private _channel?: BroadcastChannel;
    private _disposers: (() => void)[] = [];
    private _visibilityHandler = () => this._handleVisibility();
    private _redirecting = false;

    start(): void {
        if (this._started) return;

        const redirectUrl = window.sessionStorage.getItem(REDIRECT_AFTER_LOGIN) ?? "";
        if (redirectUrl) {
            window.sessionStorage.removeItem(REDIRECT_AFTER_LOGIN);
            const path = redirectUrl.replace(/^#/, "");
            if (path) {
                replaceUrl(path);
            }
        }

        if (!sessionAuthEnabled()) return;
        this._started = true;

        this._disposers.push(this._watcher.onActive(() => this._resetTimeout()));
        this._disposers.push(this._watcher.onIdle(() => this._handleIdle()));

        if (typeof BroadcastChannel !== "undefined") {
            this._channel = new BroadcastChannel(CHANNEL_NAME);
            this._channel.onmessage = ev => this._handleBroadcast(ev.data as BroadcastMessage);
        }
        document.addEventListener("visibilitychange", this._visibilityHandler);

        this._locked = false;
        if (isAuthenticated()) {
            this._watcher.setIdleDuration(idleTimeoutMs());
            this._watcher.start();
            //  Let any stale login tab know the session is live again.
            this._broadcast({ type: "authenticated" });
        }
    }

    stop(): void {
        this._watcher.stop();
        this._disposers.forEach(dispose => dispose());
        this._disposers = [];
        document.removeEventListener("visibilitychange", this._visibilityHandler);
        this._channel?.close();
        this._channel = undefined;
        this._started = false;
    }

    lock(): void {
        this._locked = true;
        this._watcher.stop();
    }

    //  Called on a server-side 401, cross-tab expiry or a stale foreground tab.
    expire(): void {
        if (this._locked || !sessionAuthEnabled()) return;
        this._locked = true;
        this._watcher.stop();
        this._broadcast({ type: "expired" });
        this._redirect(true);
    }

    private _handleIdle(): void {
        this._locked = true;
        this._watcher.stop();
        fetch("/esp/lock", { method: "post" })
            .catch(err => logger.error(err))
            .finally(() => {
                this._broadcast({ type: "locked" });
                this._redirect(true);
            });
    }

    private _resetTimeout(): void {
        if (this._locked || !isAuthenticated()) return;
        if (Date.now() - this._prevReset > SESSION_RESET_FREQ) {
            this._prevReset = Date.now();
            fetch("/esp/reset_session_timeout", { method: "post" }).catch(err => logger.error(err));
        }
    }

    private _handleVisibility(): void {
        if (document.visibilityState === "visible" && !isAuthenticated()) {
            this._locked = true;
            this._watcher.stop();
            this._redirect(true);
        }
    }

    private _handleBroadcast(msg: BroadcastMessage): void {
        if (msg?.type === "expired" || msg?.type === "locked") {
            this._locked = true;
            this._watcher.stop();
            this._redirect(true);
        }
    }

    private _broadcast(msg: BroadcastMessage): void {
        this._channel?.postMessage(msg);
    }

    private _redirect(preservePath: boolean): void {
        if (this._redirecting) return;
        this._redirecting = true;
        redirectToLogin(preservePath);
    }
}

let g_controller: SessionController | undefined;

function sessionController(): SessionController {
    if (!g_controller) {
        g_controller = new SessionController();
    }
    return g_controller;
}

//  Invoked by the logging layer when a comms request returns a 401.
export function notifySessionExpired(): void {
    g_controller?.expire();
}

export function lockSession(): void {
    sessionController().lock();
}

export function useSession(): void {
    React.useEffect(() => {
        const controller = sessionController();
        controller.start();
        return () => controller.stop();
    }, []);
}
