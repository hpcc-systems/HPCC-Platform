import * as lang from "dojo/_base/lang";
import * as arrayUtil from "dojo/_base/array";
import * as QueryResults from "dojo/store/util/QueryResults";
import {UndefinedMemory} from "./ESPUtil";

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
        var retVal = super.query(query, options);
        var sortSet = options && options.sort;
        if (sortSet) {
            retVal.sort(typeof sortSet === "function" ? sortSet : function (a, b) {
                for (var sort, i = 0; sort = sortSet[i]; i++) {
                    var aValue = a[sort.attribute];
                    var bValue = b[sort.attribute];
                    // valueOf enables proper comparison of dates
                    aValue = aValue != null ? aValue.valueOf() : aValue;
                    bValue = bValue != null ? bValue.valueOf() : bValue;
                    if (aValue !== bValue) {
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
            for (var key in item) {
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
        var width = this.cacheColumns[key] * 9;
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
        var skip = skip || [];
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
        for (var key in this.cacheColumns) {
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
                    }
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

            for (var key in item) {
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
        var filter = {};
        if (options.originalQuery.__hpcc_notActivity) {
            filter = {
                __hpcc_notActivity: true
            };
        }
        return QueryResults(this.queryEngine(filter, options)(parent._children));
    }
}
