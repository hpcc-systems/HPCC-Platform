import * as QueryResults from "dojo/store/util/QueryResults";
import * as SimpleQueryEngine from "dojo/store/util/SimpleQueryEngine";
import { alphanumSort } from "./Utility";

//  Replacement for "dojo/store/Memory"
export interface MemoryOptions {
    data?: any,
    idProperty?: string,
    index?: any,
    queryEngine?: any
}

export class Memory {

    data = null;
    idProperty = "id";
    index = null;
    queryEngine = SimpleQueryEngine;

    constructor(options: MemoryOptions = {}) {
        for (const i in options) {
            this[i] = options[i];
        }
        this.setData(this.data || []);
    }

    get(id) {
        return this.data[this.index[id]];
    }

    getIdentity(object) {
        return object[this.idProperty];
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

    query(query, options) {
        return QueryResults(this.queryEngine(query, options)(this.data));
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
}

export class AlphaNumSortMemory extends Memory {

    constructor(protected alphanumSort: { [id: string]: boolean }, options: MemoryOptions = {}) {
        super(options);
    }

    query(query, options) {
        const retVal = super.query(query, options);
        if (options?.sort && options?.sort.length && this.alphanumSort[options.sort[0].attribute]) {
            alphanumSort(retVal, options.sort[0].attribute, options.sort[0].descending);
        }
        return retVal;
    }
}