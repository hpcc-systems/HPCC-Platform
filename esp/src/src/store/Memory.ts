import * as Observable from "dojo/store/Observable";
import { SimpleQueryEngine } from "./util/SimpleQueryEngine";
import { BaseRow, QueryRequest, QueryOptions, ThenableResponse, BaseStore } from "./Store";

export * from "./Store";
export { Observable };

export class Memory<T extends BaseRow = BaseRow> extends BaseStore<T, T> {

    protected data: T[] = null;
    protected index: { [id: string | number]: number } = {};
    protected alphanumSort: { [id: string]: boolean };
    protected queryEngine = SimpleQueryEngine;

    constructor(idProperty: keyof T, alphanumSort: { [id: string]: boolean } = {}) {
        super(idProperty);
        this.setData(this.data || []);
        this.alphanumSort = alphanumSort;
    }

    get(id: string | number): T {
        return this.data[this.index[id]];
    }

    put(row, options) {
        const data = this.data;
        const index = this.index;
        const idProperty = this.responseIDField;
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

    private _dataVersion = 0;
    get dataVersion() { return this._dataVersion; }
    setData(data) {
        this._dataVersion++;
        if (data.items) {
            this.responseIDField = data.identifier || this.responseIDField;
            data = this.data = data.items;
        } else {
            this.data = data;
        }
        this.index = {};
        for (let i = 0, l = data.length; i < l; i++) {
            this.index[data[i][this.responseIDField]] = i;
        }
    }

    protected fetchData(request: QueryRequest<T>, options: QueryOptions<T>): ThenableResponse<T> {
        const data = this.queryEngine(request, options)(this.data);
        data.total = this.data.length;
        return Promise.resolve(data);
    }
}
