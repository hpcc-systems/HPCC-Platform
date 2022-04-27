import { Thenable } from "./Deferred";
import { BaseStore, QueryOptions, QueryRequest } from "./Memory";
import { BaseRow, ThenableResponse } from "./Store";

interface Page<T> extends Array<T> {
    start: number;
    size: number;
    total: number;
}

function dataPage<T>(array: T[], start: number, size: number, total: number): Page<T> {
    const retVal = array as Page<T>;
    retVal.start = start;
    retVal.size = size;
    retVal.total = total;
    return retVal;
}

type FetchData<R, T> = (query: QueryRequest<R>) => Thenable<{ data: T[], total: number }>;

export interface RequestFields<T extends BaseRow> {
    start: keyof T;
    count: keyof T;
    sortBy?: keyof T;
    descending?: keyof T;
}

export interface ResponseFields<T extends BaseRow> {
    id: keyof T;
    total: keyof T;
}

export class Paged<R extends BaseRow = BaseRow, T extends BaseRow = BaseRow> extends BaseStore<R, T> {

    protected index: { [id: string | number]: T } = {};

    private _requestFields: RequestFields<R>;
    private _fetchData: FetchData<R, T>;

    constructor(requestFields: RequestFields<R>, responseIDField: string, fetchData: FetchData<R, T>) {
        super(responseIDField);
        this._requestFields = requestFields;
        this._fetchData = fetchData;
    }

    fetchData(request: QueryRequest<R>, options: QueryOptions<T>): ThenableResponse<T> {
        if (options.start !== undefined && options.count !== undefined) {
            request[this._requestFields.start] = options.start as any;
            request[this._requestFields.count] = options.count as any;
        }
        if (options?.sort?.length && this._requestFields.sortBy && this._requestFields.descending) {
            request[this._requestFields.sortBy] = options.sort[0].attribute as any;
            request[this._requestFields.descending] = options.sort[0].descending as any;
        }
        return this._fetchData(request).then(response => {
            response.data.forEach(row => {
                this.index[this.getIdentity(row)] = row;
            });
            return dataPage(response.data, options.start, options.count, response.total);
        });
    }

    get(id: string | number): T {
        return this.index[id];
    }
}
