import UniversalRouter, { ResolveContext } from "universal-router";
import { parse, ParsedQuery, stringify } from "query-string";
import { hashSum, scopedLogger } from "@hpcc-js/util";
import { userKeyValStore } from "src/KeyValStore";

const logger = scopedLogger("../util/history.ts");

let g_router: UniversalRouter;

export function initialize(routes) {
    if (g_router) {
        console.error("g_router already initialized.");
    }
    g_router = new UniversalRouter(routes);
    return g_router;
}

export function resolve(pathnameOrContext: string | ResolveContext) {
    return g_router.resolve(pathnameOrContext);
}

function parseHash(hash: string): HistoryLocation {
    if (hash[0] !== "#") {
        return {
            pathname: "/",
            search: "",
            id: hashSum("#/")
        };
    }

    const parts = hash.substring(1).split("?");
    return {
        pathname: parts[0],
        search: parts.length > 1 ? `?${parts[1]}` : "",
        id: hashSum(hash)
    };
}

export function parseSearch<T = ParsedQuery<string | boolean | number>>(_: string): T {
    if (_[0] !== "?") return {} as T;
    return { ...parse(_.substring(1), { parseBooleans: true, parseNumbers: true }) } as unknown as T;
}

interface HistoryLocation {
    pathname: string;
    search: string;
    id: string;
}

export type ListenerCallback<S extends object = object> = (location: HistoryLocation, action: string) => void;

const globalHistory = globalThis.history;

const STORE_HISTORY_ID = "history";

class History<S extends object = object> {

    location: HistoryLocation = {
        pathname: "/",
        search: "",
        id: hashSum("#/")
    };
    state: S = {} as S;
    _store = userKeyValStore();

    constructor() {
        this.location = parseHash(document.location.hash);

        window.addEventListener("hashchange", ev => {
            const prevID = this.location.id;
            this.location = parseHash(document.location.hash);
            if (prevID !== this.location.id) {
                this.state = {} as S;
            }
            this.broadcast("HASHCHANGE");
        });

        window.addEventListener("popstate", ev => {
            logger.debug("popstate: " + document.location + ", state: " + JSON.stringify(ev.state));
            this.state = ev.state;
        });

        this._store.get(STORE_HISTORY_ID).then((str: string) => {
            if (typeof str === "string") {
                const retVal: HistoryLocation[] = JSON.parse(str);
                if (Array.isArray(retVal)) {
                    this._recent = retVal;
                }
            }
        }).catch(logger.error).finally(() => {
            this._recent = this._recent === undefined ? [] : this._recent;
        });
    }

    push(to: { pathname?: string, search?: string }, state?: S) {
        const newHash = `#${to.pathname || this.location.pathname}${to.search || ""}`;
        globalHistory.pushState(state, "", newHash);
        this.location = parseHash(newHash);
        this.broadcast("PUSH");
    }

    replace(to: { pathname?: string, search?: string }, state?: S) {
        const newHash = `#${to.pathname || this.location.pathname}${to.search || ""}`;
        globalHistory.replaceState(state, "", newHash);
        this.location = parseHash(newHash);
        this.broadcast("REPLACE");
    }

    _listenerID = 0;
    _listeners: { [id: number]: ListenerCallback } = {};
    listen(callback: ListenerCallback) {
        const id = ++this._listenerID;
        this._listeners[id] = callback;
        return () => {
            delete this._listeners[id];
        };
    }

    protected _recent;
    recent() {
        return this._recent === undefined ? [] : this._recent;
    }

    updateRecent() {
        if (this._recent !== undefined) {
            this._recent = this._recent?.filter(row => row.id !== this.location.id) || [];
            this._recent.unshift(this.location);
            if (this._recent.length > 10) {
                this._recent.length = 10;
            }
            this._store.set(STORE_HISTORY_ID, JSON.stringify(this._recent)).catch(logger.error);
        }
    }

    broadcast(action: string) {
        this.updateRecent();
        for (const key in this._listeners) {
            const listener = this._listeners[key];
            listener(this.location, action);
        }
    }
}

export const hashHistory = new History<any>();

export function pushSearch(_: object, state?: any) {
    const search = stringify(_ as any);
    hashHistory.push({
        search: search ? "?" + search : ""
    }, state);
}

export function pushUrl(_: string, state?: any) {
    hashHistory.push({
        pathname: _
    }, state);
}

export function updateSearch(_: object, state?: any) {
    const search = stringify(_ as any);
    hashHistory.replace({
        search: search ? "?" + search : ""
    }, state);
}

export function pushParam(key: string, val?: string | string[] | number | boolean, state?: any) {
    pushParams({ [key]: val }, state);
}

export function pushParamExact(key: string, val?: string | string[] | number | boolean, state?: any) {
    pushParams({ [key]: val }, state, true);
}

export function pushParams(search: { [key: string]: string | string[] | number | boolean }, state?: any, keepEmpty: boolean = false) {
    const params = parseSearch(hashHistory.location.search);
    for (const key in search) {
        const val = search[key];
        //  No empty strings OR "false" booleans...
        if (!keepEmpty && (val === "" || val === false)) {
            delete params[key];
        } else {
            params[key] = val;
        }
    }
    pushSearch(params, state);
}

export function updateParam(key: string, val?: string | string[] | number | boolean, state?: any) {
    const params = parseSearch(hashHistory.location.search);
    if (val === undefined) {
        delete params[key];
    } else {
        params[key] = val;
    }
    updateSearch(params, state);
}
