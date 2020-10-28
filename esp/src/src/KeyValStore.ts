import { detect } from "detect-browser";
import { Store, ValueChangedMessage } from "@hpcc-js/comms";
import { Dispatch, IObserverHandle } from "@hpcc-js/util";

declare const dojoConfig;

export interface IKeyValStore {
    set(key: string, value: string, broadcast?: boolean): Promise<void>;
    get(key: string, broadcast?: boolean): Promise<string | undefined>;
    getAll(broadcast?: boolean): Promise<{ [key: string]: string }>;
    delete(key: string, broadcast?: boolean): Promise<void>;
    monitor(callback: (messages: ValueChangedMessage[]) => void): IObserverHandle;
}

/**
 *  Global Store
 *      Stores info in Dali, ignores user ID
 *      No push notifications outside of current tab
 */
export function globalKeyValStore(): IKeyValStore {
    return Store.attach({ baseUrl: "" }, "HPCCApps", "ECLWatch", false);
}

// Initialize Global Store  ---
const store = globalKeyValStore();
store.set("", "", false);

// Grab some aprox metrics - ignoring obvious race condition
const browser = detect();
const majorVersion = browser.version.split(".")[0];
const now = new Date(Date.now()).toISOString();
store.get("browser-stats").then(statsStr => {
    try {
        const stats = JSON.parse(statsStr || "{}") || {};
        if (!stats.since) stats.since = now;
        if (browser.type === "browser") {
            //  Browser Stats  ---
            if (!stats[browser.name]) stats[browser.name] = {};
            if (!stats[browser.name][majorVersion]) stats[browser.name][majorVersion] = {};
            stats[browser.name][majorVersion].lastSeen = now;

            if (!stats[browser.name][majorVersion].count) stats[browser.name][majorVersion].count = 0;
            stats[browser.name][majorVersion].count++;

            //  OS Stats  ---
            if (!stats[browser.os]) stats[browser.os] = {};
            stats[browser.os].lastSeen = now;

            if (!stats[browser.os].count) stats[browser.os].count = 0;
            stats[browser.os].count++;

            store.set("browser-stats", JSON.stringify(stats), false);
        }
    } catch (e) {
        console.warn("Failed to wrtie stats", e);
    }
});

export function fetchStats() {
    const store = globalKeyValStore();
    return store.get("browser-stats").then(statsStr => {
        const browser = [];
        const os = [];
        try {
            const stats = JSON.parse(statsStr);
            for (const key in stats) {
                if (key !== "since") {
                    const val = stats[key];
                    if (val.count === undefined) {
                        for (const bKey in val) {
                            browser.push([`${key}-${bKey}`, val[bKey].count]);
                        }
                    } else {
                        os.push([`${key}`, val.count]);
                    }
                }
            }
        } catch (e) {
            console.warn("Failed to read stats", e);
        }
        return {
            browser,
            os
        };
    });
}

export function getRecentFilters(filterName) {
    const store = userKeyValStore();

    return store.get(filterName).then(response =>{
        let results;
        try {
            results = JSON.parse(response);
        } catch(e) {
            console.warn("Failed to read recent filters", e);
        }
        return results;
    });
}

/**
 *  User Store
 *      Stores info in Dali by user ID
 *      No push notifications outside of current tab
 */
export function userKeyValStore(): IKeyValStore {
    const userName = dojoConfig.username;
    if (!userName) {
        //  Fallback to local storage  ---
        return localKeyValStore();
    }
    return Store.attach({ baseUrl: "" }, "HPCCApps", "ECLWatch", true);
}

class LocalStorage implements IKeyValStore {

    protected _storage = window.localStorage;

    protected _dispatch = new Dispatch();
    protected _prefix: string;
    protected _prefixLength: number;

    constructor(prefix: string = "ECLWatch") {
        this._prefix = prefix;
        this._prefixLength = this._prefix.length;

        if (typeof StorageEvent !== void (0)) {
            window.addEventListener("storage", (event: StorageEvent) => {
                if (this.isECLWatchKey(event.key)) {
                    this._dispatch.post(new ValueChangedMessage(this.extractKey(event.key), event.newValue, event.oldValue));
                }
            });
        } else {
            console.log("Browser doesn't support multi-tab communication");
        }
    }

    isECLWatchKey(key: string): boolean {
        return key.indexOf(`${this._prefix}:`) === 0;
    }

    extractKey(key: string): string {
        return key.substring(this._prefixLength + 1);
    }

    set(key: string, value: string, broadcast?: boolean): Promise<void> {
        const oldValue = this._storage.getItem(`${this._prefix}:${key}`);
        this._storage.setItem(`${this._prefix}:${key}`, value);
        return Promise.resolve().then(() => {
            if (broadcast) {
                this._dispatch.post(new ValueChangedMessage(key, value, oldValue));
            }
        });
    }

    get(key: string, broadcast?: boolean): Promise<string | undefined> {
        const value = this._storage.getItem(`${this._prefix}:${key}`);
        return Promise.resolve(value);
    }

    getAll(broadcast?: boolean): Promise<{ [key: string]: string }> {
        const retVal: { [key: string]: string } = {};
        for (let i = 0; i < this._storage.length; ++i) {
            const key = this._storage.key(i);
            if (this.isECLWatchKey(key)) {
                retVal[this.extractKey(key)] = this._storage.getItem(key);
            }
        }
        return Promise.resolve(retVal);
    }

    delete(key: string, broadcast?: boolean): Promise<void> {
        const oldValue = this._storage.getItem(`${this._prefix}:${key}`);
        this._storage.removeItem(key);
        return Promise.resolve().then(() => {
            if (broadcast) {
                this._dispatch.post(new ValueChangedMessage(key, undefined, oldValue));
            }
        });
    }

    monitor(callback: (messages: ValueChangedMessage[]) => void): IObserverHandle {
        return this._dispatch.attach(callback);
    }
}

let _localStorage: LocalStorage;

/**
 *  Local Store
 *      Stores info in local storage
 *      Includes push notifications outside of current tab
 */
export function localKeyValStore(): IKeyValStore {
    if (!_localStorage) {
        _localStorage = new LocalStorage();
    }
    return _localStorage;
}

class SessionStorage extends LocalStorage {

    protected _storage = window.sessionStorage;

    constructor() {
        super();
    }
}

let _sessionStorage: SessionStorage;

/**
 *  Session Store
 *      Stores info in session storage
 *      Includes push notifications outside of current tab
 */
export function sessionKeyValStore(): IKeyValStore {
    if (!_sessionStorage) {
        _sessionStorage = new SessionStorage();
    }
    return _sessionStorage;
}
