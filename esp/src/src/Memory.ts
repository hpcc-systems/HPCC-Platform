import * as Deferred from "dojo/Deferred";
import * as Observable from "dojo/store/Observable";
import * as QueryResults from "dojo/store/util/QueryResults";
import * as SimpleQueryEngine from "dojo/store/util/SimpleQueryEngine";
import { alphanumSort } from "./Utility";

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

    query(query, options) {
        const retVal = new Deferred();
        this.fetchData().then(response => {
            const data = this.queryEngine(query, options)(response);
            retVal.resolve(data);
        });
        return QueryResults(retVal.then(response => response), {
            totalLength: retVal.then(response => response.length)
        });
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

    setData(data) {
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
        const retVal = super.query(query, options);
        if (options?.sort && options?.sort.length && this.alphanumSort[options.sort[0].attribute]) {
            alphanumSort(retVal, options.sort[0].attribute, options.sort[0].descending);
        }
        return retVal;
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
