import * as arrayUtil from "dojo/_base/array";
import * as lang from "dojo/_base/lang";
import * as QueryResults from "dojo/store/util/QueryResults";
import { UndefinedMemory } from "./ESPUtil";

export class GraphStore extends UndefinedMemory {
    idProperty: string;
    cacheColumns = {};

    constructor(idProperty: string = "id") {
        super();
        this.idProperty = idProperty;
    }

    setData(data) {
        super.setData(data);
        this.cacheColumns = {};
        this.calcColumns();
    }

    query(query, options) {
        const retVal = super.query(query, options);
        const sortSet = options && options.sort;
        if (sortSet) {
            retVal.sort(typeof sortSet === "function" ? sortSet : function (a, b) {
                // tslint:disable-next-line: no-conditional-assignment
                for (let sort, i = 0; sort = sortSet[i]; i++) {
                    let aValue = a[sort.attribute];
                    let bValue = b[sort.attribute];
                    // valueOf enables proper comparison of dates
                    aValue = aValue != null ? aValue.valueOf() : aValue;
                    bValue = bValue != null ? bValue.valueOf() : bValue;
                    if (aValue !== bValue) {
                        // tslint:disable-next-line: triple-equals
                        return !!sort.descending == (bValue == null || aValue > bValue) ? -1 : 1;   // jshint ignore:line
                    }
                }
                return 0;
            });
        }
        return retVal;
    }

    //  Helpers  ---
    isNumber(n) {
        return !isNaN(parseFloat(n)) && isFinite(n);
    }

    calcColumns() {
        arrayUtil.forEach(this.data, function (item, idx) {
            for (const key in item) {
                if (key !== "id" && key.substring(0, 1) !== "_") {
                    if (!this.cacheColumns[key]) {
                        this.cacheColumns[key] = item[key].length;
                    } else if (item[key].length > this.cacheColumns[key]) {
                        this.cacheColumns[key] = item[key].length;
                    }
                }
                if (this.isNumber(item[key])) {
                    item[key] = parseFloat(item[key]);
                }
            }
        }, this);
    }

    getColumnWidth(key) {
        let width = this.cacheColumns[key] * 9;
        if (width < 27) {
            width = 27;
        } else if (width > 300) {
            width = 300;
        }
        return width;
    }

    appendColumns(target, highPriority: string[], lowPriority: string[], skip: string[] = [], formatTime: boolean = false) {
        if (!highPriority) {
            highPriority = [];
        }
        if (!lowPriority) {
            lowPriority = [];
        }
        skip = skip || [];
        arrayUtil.forEach(target, function (item, idx) {
            skip.push(item.field);
        });
        arrayUtil.forEach(highPriority, function (key, idx) {
            if (skip.indexOf(key) === -1 && this.cacheColumns[key]) {
                target.push({
                    field: key, label: key, width: this.getColumnWidth(key)
                });
            }
        }, this);
        for (const key in this.cacheColumns) {
            if (skip.indexOf(key) === -1 && highPriority.indexOf(key) === -1 && lowPriority.indexOf(key) === -1 && key.substring(0, 1) !== "_") {
                target.push({
                    field: key, label: key, width: this.getColumnWidth(key)
                });
            }
        }
        arrayUtil.forEach(lowPriority, function (key, idx) {
            if (skip.indexOf(key) === -1 && this.cacheColumns[key]) {
                target.push({
                    field: key, label: key, width: this.getColumnWidth(key)
                });
            }
        }, this);
        if (formatTime) {
            arrayUtil.forEach(target, function (column, idx) {
                if (column.label.indexOf("Time") === 0 || column.label.indexOf("Size") === 0 || column.label.indexOf("Skew") === 0) {
                    column.formatter = function (_id, row) {
                        return row["_" + column.field] || "";
                    };
                }
            });
        }
    }
}

export class GraphTreeStore extends GraphStore {
    idProperty = "id";

    //  Store API  ---
    constructor() {
        super();
    }

    query(query, options) {
        return super.query(query, options);
    }

    setTree(data) {
        this.setData([]);
        this.cacheColumns = {};
        this.walkData(data);
    }

    walkData(data) {
        arrayUtil.forEach(data, function (item, idx) {
            if (item._children) {
                item._children.sort(function (l, r) {
                    return l.id - r.id;
                });
                this.walkData(item._children);
                lang.mixin(item, {
                    __hpcc_notActivity: true
                });
            }
            this.add(item);

            for (const key in item) {
                if (key !== "id" && key.substring(0, 1) !== "_") {
                    if (!this.cacheColumns[key]) {
                        this.cacheColumns[key] = item[key].length;
                    } else if (item[key].length > this.cacheColumns[key]) {
                        this.cacheColumns[key] = item[key].length;
                    }
                }
                if (this.isNumber(item[key])) {
                    item[key] = parseFloat(item[key]);
                }
            }
        }, this);
    }

    //  Tree API  ---
    mayHaveChildren(object) {
        return object._children;
    }

    getChildren(parent, options) {
        let filter = {};
        if (options.originalQuery.__hpcc_notActivity) {
            filter = {
                __hpcc_notActivity: true
            };
        }
        return QueryResults(this.queryEngine(filter, options)(parent._children));
    }
}
