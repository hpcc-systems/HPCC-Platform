import * as Observable from "dojo/store/Observable";
import * as QueryResults from "dojo/store/util/QueryResults";
import * as SimpleQueryEngine from "dojo/store/util/SimpleQueryEngine";
import { alphanum } from "./Utility";

class Deferred<T> {

    promise: Promise<T>;
    resolve: (value: T | PromiseLike<T>) => void;
    reject: (reason?: any) => void;

    protected _isCancelled = false;
    protected _cancelledReason: string;
    protected _isResolved = false;
    protected _isRejected = false;

    constructor() {
        this.promise = new Promise((resolve, reject) => {
            this.resolve = resolve;
            this.reject = reject;
        });
    }

    then(onFulfilled?: (value: T) => void, onRejected?: (reason: any) => void): Deferred<T> {
        this.promise.then((value: T) => {
            if (this._isCancelled && onRejected) {
                onRejected(this._cancelledReason);
            } else if (!this._isCancelled && onFulfilled) {
                this._isResolved = true;
                onFulfilled(value);
            }
        }, (reason: any) => {
            if (this._isCancelled && onRejected) {
                onRejected(this._cancelledReason);
            } else if (!this._isCancelled && onRejected) {
                this._isRejected = true;
                onRejected(reason);
            }
        });
        return this;
    }

    cancel(reason?: string) {
        this._isCancelled = true;
        this._cancelledReason = reason;
    }

    isResolved() {
        return this._isResolved;
    }

    isRejected() {
        this._isRejected;
    }

    isFulfilled() {
        this._isResolved || this._isRejected || this._isCancelled;
    }

    isCanceled() {
        this._isCancelled;
    }
}

class DeferredList<T> extends Deferred<T[]> {
    total: Deferred<number>;
}

export {
    Observable
};

//  Replacement for "dojo/store/Memory"
export interface MemoryOptions {
    data?: any,
    idProperty?: string,
    index?: any,
    queryEngine?: any
}

type FetchDataResponse = Promise<any[]>;

export class BaseStore {

    protected idProperty: string;
    protected index = null;
    protected queryEngine = SimpleQueryEngine;

    constructor(idProperty: string = "id") {
        this.idProperty = idProperty;
    }

    getIdentity(object) {
        return object[this.idProperty];
    }

    protected fetchData(): FetchDataResponse {
        return Promise.resolve([]);
    }

    query(query, options: { start: number, count: number, sort: { attribute: string, descending: boolean } }): QueryResults<any> {
        const retVal = new DeferredList();
        retVal.total = new Deferred();
        this.fetchData().then(response => {
            retVal.resolve(this.queryEngine(query, options)(response));
            retVal.total.resolve(response.length);
        });
        return new QueryResults(retVal);
    }
}

export class Memory extends BaseStore {

    protected data = null;

    constructor(idProperty: string = "id") {
        super(idProperty);
        this.setData(this.data || []);
    }

    get(id: string) {
        return this.data[this.index[id]];
    }

    put(row, options) {
        const data = this.data;
        const index = this.index;
        const idProperty = this.idProperty;
        const id = row[idProperty] = (options && "id" in options) ? options.id : idProperty in row ? row[idProperty] : Math.random();
        if (id in index) {
            if (options && options.overwrite === false) {
                throw new Error("Object already exists");
            }
            data[index[id]] = row;
        } else {
            index[id] = data.push(row) - 1;
        }
        return id;
    }

    add(object, options) {
        (options = options || {}).overwrite = false;
        return this.put(object, options);
    }

    remove(id) {
        const index = this.index;
        const data = this.data;
        if (id in index) {
            data.splice(index[id], 1);
            this.setData(data);
            return true;
        }
    }

    dataVersion = 0;
    setData(data) {
        this.dataVersion++;
        if (data.items) {
            this.idProperty = data.identifier || this.idProperty;
            data = this.data = data.items;
        } else {
            this.data = data;
        }
        this.index = {};
        for (let i = 0, l = data.length; i < l; i++) {
            this.index[data[i][this.idProperty]] = i;
        }
    }

    protected fetchData(): FetchDataResponse {
        return Promise.resolve(this.data);
    }
}

export class AlphaNumSortMemory extends Memory {

    constructor(idProperty: string = "id", protected alphanumSort: { [id: string]: boolean }) {
        super(idProperty);
    }

    query(query, options) {
        if (options?.sort && options?.sort.length && this.alphanumSort[options.sort[0].attribute]) {
            const col = options.sort[0].attribute;
            const reverse = options.sort[0].descending;
            return super.query(query, {
                ...options,
                sort: function (l, r) {
                    return alphanum(l[col], r[col]) * (reverse ? -1 : 1);
                }
            });
        }
        return super.query(query, options);
    }
}

export class ASyncStore extends BaseStore {

    constructor(idProperty: string = "id", protected _fetchData: () => Promise<void | any[]>) {
        super(idProperty);
    }

    fetchData(): FetchDataResponse {
        return this._fetchData().then(data => !!data ? data : []);
    }
}
