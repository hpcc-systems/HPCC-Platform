import { parse, stringify } from "query-string";

class SortBy {
    readonly attribute: string = "";
    readonly descending: boolean = false;

    constructor(sortBy: string);
    constructor(attribute: string, descending: boolean);
    constructor(attribute_sortBy: string, descending: boolean = false) {
        if (descending === undefined) {
            this.attribute = attribute_sortBy;
            this.descending = false;
            if (attribute_sortBy.charAt(0) === "-") {
                this.attribute = attribute_sortBy.substring(1);
                this.descending = true;
            }
        } else {
            this.attribute = attribute_sortBy;
            this.descending = descending;
        }
    }

    serialize(): string {
        return (this.descending ? "-" : "") + this.attribute;
    }

    static deserialize(sortBy: string): SortBy {
        return new SortBy(sortBy);
    }

}

export type ParamValue = string | boolean | number | null;
export interface Params {
    sortBy?: string;
    pageNum?: number;
    fullscreen?: boolean;
    [key: string]: ParamValue | Array<ParamValue>;
}

export class SearchParams {
    readonly params: Params = {};

    constructor();
    constructor(search: string);
    constructor(params: Params);
    constructor(search?: string | Params) {
        if (typeof search === "string") {
            this.params = parse(search, { parseBooleans: true, parseNumbers: true });
        } else if (search) {
            this.params = search;
        }
    }

    serialize(): string {
        return stringify(this.params, {
            encode: false,
            skipEmptyString: true
        });
    }

    static deserialize(search: string): SearchParams {
        return new SearchParams(search);
    }

    sortBy(): SortBy | undefined;
    sortBy(sortBy: SortBy | null): this;
    sortBy(sortBy?: SortBy | null): SortBy | undefined | this {
        if (sortBy !== undefined) {
            if (sortBy === null) {
                delete this.params["sortBy"];
            } else {
                this.params["sortBy"] = sortBy.serialize();
            }
            return this;
        }
        return this.params["sortBy"] ? SortBy.deserialize(this.params["sortBy"]) : undefined;
    }

    pageNum(): number | undefined;
    pageNum(pageNum: number | null): this;
    pageNum(pageNum?: number | null): number | undefined | this {
        if (pageNum !== undefined) {
            if (pageNum === null) {
                delete this.params["pageNum"];
            } else {
                this.params["pageNum"] = pageNum;
            }
            return this;
        }
        return this.params["pageNum"];
    }

    fullscreen(): boolean | undefined;
    fullscreen(fullscreen: boolean): this;
    fullscreen(fullscreen?: boolean): boolean | undefined | this {
        if (fullscreen !== undefined) {
            if (fullscreen === false) {
                delete this.params["fullscreen"];
            } else {
                this.params["fullscreen"] = null;
            }
            return this;
        }
        return this.params["fullscreen"] === null || this.params["fullscreen"] === true;
    }

    param(key: string): ParamValue | ParamValue[] | undefined;
    param(key: string, val: ParamValue | null): this;
    param(key: string, val?: ParamValue | null): ParamValue | ParamValue[] | undefined | this {
        if (val !== undefined) {
            if (val === null) {
                delete this.params[key];
            } else {
                switch (key) {
                    case "sortBy":
                        this.sortBy(new SortBy(val as string));
                        break;
                    case "pageNum":
                        this.pageNum(val as number);
                        break;
                    case "fullscreen":
                        this.fullscreen(val as boolean);
                        break;
                    default:
                        this.params[key] = val;
                }
            }
            return this;
        }
        switch (key) {
            case "sortBy":
                return this.sortBy()?.serialize();
            case "pageNum":
                return this.pageNum(val as number);
            case "fullscreen":
                return this.fullscreen();
        }
        return this.params[key];
    }
}
