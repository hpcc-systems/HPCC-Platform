import * as React from "react";
import { CallbackFunction, Observable } from "@hpcc-js/util";
import { userKeyValStore } from "src/KeyValStore";
import { IContextualMenuItem } from "@fluentui/react";
import { hashHistory } from "../util/history";

const STORE_FAVORITES_ID = "favorites";
const STORE_CACHE_TIMEOUT = 10000;

export function resetFavorites() {
    const store = userKeyValStore();
    return store?.delete(STORE_FAVORITES_ID);
}

interface Payload {
    //  TODO:  Will be used for labels and extra info...
}
type UrlMap = { [url: string]: Payload };

class Favorites {

    private _store = userKeyValStore();
    private _observable = new Observable("cleared", "added", "removed");

    constructor() {
    }

    private _prevPull: Promise<UrlMap>;
    private async pull(): Promise<UrlMap> {
        if (!this._prevPull) {
            this._prevPull = this._store.get(STORE_FAVORITES_ID).then((str: string): UrlMap => {
                if (typeof str === "string") {
                    try {
                        const retVal = JSON.parse(str);
                        if (retVal.constructor === Object) {
                            return retVal;
                        }
                    } catch (e) {
                        return {};
                    }
                }
                return {};
            });
            setTimeout(() => delete this._prevPull, STORE_CACHE_TIMEOUT);
        }
        return this._prevPull;
    }

    private async push(favs: UrlMap): Promise<void> {
        this._prevPull = Promise.resolve(favs);
        return this._store.set(STORE_FAVORITES_ID, JSON.stringify(favs));
    }

    async clear(): Promise<void> {
        this.push({});
        this._observable.dispatchEvent("cleared");
    }

    async has(url: string): Promise<boolean> {
        const favs = await this.pull();
        return favs[url] !== undefined;
    }

    async add(url: string, payload: Payload = {}): Promise<void> {
        const favs = await this.pull();
        favs[url] = payload;
        this.push(favs);
        this._observable.dispatchEvent("added", url);
    }

    async remove(url: string): Promise<void> {
        const favs = await this.pull();
        delete favs[url];
        this.push(favs);
        this._observable.dispatchEvent("removed", url);
    }

    async all(): Promise<UrlMap> {
        return await this.pull();
    }

    listen(callback: CallbackFunction): () => void {
        const added = this._observable.addObserver("added", val => callback("added", val));
        const removed = this._observable.addObserver("removed", val => callback("removed", val));
        return () => {
            added.release();
            removed.release();
        };
    }
}
const favorites = new Favorites();

export function useFavorite(hash: string): [boolean, () => void, () => void] {
    const [favorite, setFavorite] = React.useState(false);

    React.useEffect(() => {
        favorites.has(hash).then(setFavorite);
        return favorites?.listen(() => {
            favorites.has(hash).then(setFavorite);
        });
    }, [hash]);

    return [favorite, () => favorites.add(hash), () => favorites.remove(hash)];
}

export function useFavorites(): [UrlMap] {
    const [all, setAll] = React.useState<UrlMap>({});

    React.useEffect(() => {
        favorites.all().then(all => setAll({ ...all }));
        return favorites?.listen(async () => {
            favorites.all().then(all => setAll({ ...all }));
        });
    }, []);

    return [all];
}

export function useHistory(): [IContextualMenuItem[]] {

    const [history, setHistory] = React.useState<IContextualMenuItem[]>([]);

    React.useEffect(() => {
        return hashHistory.listen((location, action) => {
            setHistory(hashHistory.recent().map((row): IContextualMenuItem => {
                const url = `#${row.pathname + row.search}`;
                return {
                    name: decodeURI(row.pathname),
                    href: url,
                    key: url
                };
            }));
        });
    }, []);

    return [history];
}
