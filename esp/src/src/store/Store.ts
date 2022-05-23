import * as QueryResults from "dojo/store/util/QueryResults";
import { DeferredResponse, Thenable } from "./Deferred";

//  Query  ---
export type Key = string | number | symbol;
export type BaseRow = { [id: Key]: any; }

export type QueryRequest<T extends BaseRow = BaseRow> = Partial<T>;
export interface QueryResponse<T> extends Array<T> {
    total: number;
}

export interface ThenableResponse<T> extends Thenable<QueryResponse<T>> {
}

export type QuerySortFunction<T> = (a: T, b: T) => number;
export type QuerySortItem<T extends BaseRow = BaseRow> = { attribute: keyof T, descending: boolean };
export type QuerySort<T> = QuerySortFunction<T> | QuerySortItem<T>[];

export interface QueryOptions<T> {
    start?: number;
    count?: number;
    sort?: QuerySort<T>
}

export abstract class BaseStore<R extends BaseRow, T extends BaseRow> {

    protected responseIDField: keyof T;

    constructor(responseIDField: keyof T) {
        this.responseIDField = responseIDField;
    }

    protected abstract fetchData(request: QueryRequest<R>, options: QueryOptions<T>): ThenableResponse<T>;

    abstract get(id: string | number): T;

    getIdentity(object): string | number {
        return object[this.responseIDField];
    }

    protected query(request: QueryRequest<R>, options: QueryOptions<T>): DeferredResponse<T> {
        const retVal = new DeferredResponse<T>();
        this.fetchData(request, options).then((data: QueryResponse<T>) => {
            retVal.total.resolve(data.total);
            retVal.resolve(data);
        });
        return QueryResults(retVal);
    }
}

