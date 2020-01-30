import * as declare from "dojo/_base/declare";
import * as lang from "dojo/_base/lang";
import "dojo/i18n";
// @ts-ignore
import * as nlsHPCC from "dojo/i18n!hpcc/nls/hpcc";
import * as arrayUtil from "dojo/_base/array";
import * as domClass from "dojo/dom-class";
import * as domStyle from "dojo/dom-style";
import * as Stateful from "dojo/Stateful";
import * as query from "dojo/query";
import * as json from "dojo/json";
import * as aspect from "dojo/aspect";
import * as Evented from "dojo/Evented";
import * as on from "dojo/on";
import * as Memory from "dojo/store/Memory";

import * as registry from "dijit/registry";
import * as Tooltip from "dijit/Tooltip";

// @ts-ignore
import * as DGrid from "dgrid/Grid";
// @ts-ignore
import * as OnDemandGrid from "dgrid/OnDemandGrid";
// @ts-ignore
import * as CompoundColumns from 'dgrid/extensions/CompoundColumns';
// @ts-ignore
import * as Keyboard from "dgrid/Keyboard";
// @ts-ignore
import * as Selection from "dgrid/Selection";
// @ts-ignore
import * as ColumnResizer from "dgrid/extensions/ColumnResizer";
// @ts-ignore
import * as ColumnHider from "dgrid/extensions/ColumnHider";
// @ts-ignore
import * as DijitRegistry from "dgrid/extensions/DijitRegistry";

import { select as d3Select } from "d3-selection";
import { Pagination } from "./Pagination";

import { declareDecorator } from './DeclareDecorator';

declare const dojo;

var SingletonData = declare([Stateful], {
    //  Attributes  ---
    _hasCompletedSetter: function (hasCompleted) {
        if (this.hasCompleted !== hasCompleted) {
            this.hasCompleted = hasCompleted;
            if (!this.hasCompleted) {
                if (this.startMonitor) {
                    this.startMonitor();
                }
            } else {
                if (this.stopMonitor) {
                    this.stopMonitor();
                }
            }
        }
    },

    //  Methods  ---
    constructor: function (args) {
        this.__hpcc_changedCount = 0;
        this.__hpcc_changedCache = {};
    },
    getData: function () {
        if (this instanceof SingletonData) {
            return (SingletonData)(this);
        }
        return this;
    },
    updateData: function (response) {
        var changed = false;
        for (var key in response) {
            if (response[key] !== undefined && response[key] !== null) {
                var jsonStr = json.stringify(response[key]);
                if (this.__hpcc_changedCache[key] !== jsonStr) {
                    this.__hpcc_changedCache[key] = jsonStr;
                    this.set(key, response[key]);
                    changed = true;
                }
            }
        }
        if (changed) {
            try {
                this.set("__hpcc_changedCount", this.get("__hpcc_changedCount") + 1);
            } catch (e) {
                /*  __hpcc_changedCount can notify a dgrid instance that a row has changed.  
                *   There is an issue (TODO check issue number) with dgrid which can cause an exception to be thrown during the notify.
                *   By catching these exceptions here normal execution can continue.
                */
            }
        }
    }
});

export var Monitor = declare(null, {
    isMonitoring: function () {
        return this._timer && this._timer > 0;
    },
    disableMonitor: function (disableMonitor) {
        this._disableMonitor = disableMonitor;
        if (!this._disableMonitor) {
            this.refresh();
            this.onMonitor();
        }
    },
    startMonitor: function (aggressive) {
        if (this.isMonitoring())
            return;

        this._timerTickCount = aggressive ? 0 : Math.floor((Math.random() * 40) + 70);
        this._timer = 1000;
        this.onMonitor();
    },
    stopMonitor: function () {
        this._timerTickCount = 0;
        this._timer = 0;
    },
    onMonitor: function () {
        if (this._disableMonitor) {
            return;
        }
        this._timerTickCount++;

        if (this.hasCompleted) {
            this.stopMonitor();
            return;
        } else {
            if (this._timerTickCount === 1) {
                this.refresh(true);
            } else if (this._timerTickCount < 5 && this._timerTickCount % 1 === 0) {
                this.refresh();
            } else if (this._timerTickCount < 30 && this._timerTickCount % 5 === 0) {
                this.refresh();
            } else if (this._timerTickCount < 60 && this._timerTickCount % 10 === 0) {
                this.refresh();
            } else if (this._timerTickCount < 120 && this._timerTickCount % 30 === 0) {
                this.refresh(true);
            } else if (this._timerTickCount % 60 === 0) {
                this.refresh(true);
            }
        }

        var context = this;
        if (this._timer) {
            setTimeout(function () {
                context.onMonitor();
            }, this._timer);
        } else {
            this._timerTickCount = 0;
        }
    }
});

var GridHelper = declare(null, {
    allowTextSelection: true,
    noDataMessage: "<span class='dojoxGridNoData'>" + nlsHPCC.noDataMessage + "</span>",
    loadingMessage: "<span class='dojoxGridNoData'>" + nlsHPCC.loadingMessage + "</span>",

    scrollTo: override(function (inherited, pos) {
        return inherited({ x: this.__xpos || pos.x, y: pos.y });
    }),

    refresh: override(function (inherited) {
        const scroller = d3Select(this.domNode).select(".dgrid-scroller");
        this.__xpos = scroller.property("scrollLeft");
        const context = this;
        return inherited(arguments).then(function () {
            scroller.property("scrollLeft", context.__xpos);
            delete context.__xpos;
        });
    }),

    postCreate: override(function (inherited) {
        inherited(arguments);

        this.__hpcc_tooltip_header = new Tooltip({
            connectId: [this.id],
            selector: ".dgrid-resize-header-container",
            showDelay: 400,
            getContent(node) {
                if (node.offsetWidth < node.scrollWidth - 4) {
                    return node.innerHTML;
                }
                return "";
            }
        });
        this.__hpcc_tooltip_header.position = ["above-centered", "below-centered", "before-centered", "after-centered"];

        this.__hpcc_tooltip = new Tooltip({
            connectId: [this.id],
            selector: "td",
            showDelay: 400,
            getContent(node) {
                if (node.offsetWidth <= node.scrollWidth) {
                    return node.innerHTML;
                }
                return "";
            }
        });
        this.__hpcc_tooltip.position = ["above-centered", "below-centered", "before-centered", "after-centered"];
    }),

    _onNotify: override(function (inherited, object, existingId) {
        inherited(arguments);
        if (this.onSelectedChangedCallback) {
            this.onSelectedChangedCallback();
        }
    }),

    onSelectionChanged: function (callback) {
        this.onSelectedChangedCallback = callback;
        this.on("dgrid-select, dgrid-deselect, dgrid-refresh-complete", function (event) {
            callback(event);
        });
    },

    clearSelection: override(function (inherited) {
        inherited(arguments);
        query("input[type=checkbox]", this.domNode).forEach(function (node) {
            node.checked = false;
            node.indeterminate = false;
        });
    }),

    setSelection: function (arrayOfIDs) {
        this.clearSelection();
        var context = this;
        arrayUtil.forEach(arrayOfIDs, function (item, idx) {
            if (idx === 0) {
                var row = context.row(item);
                if (row.element) {
                    row.element.scrollIntoView();
                }
            }
            context.select(item);
        });
    },

    setSelected: function (items) {
        this.clearSelection();
        var context = this;
        arrayUtil.forEach(items, function (item, idx) {
            if (idx === 0) {
                var row = context.row(item);
                if (row.element) {
                    row.element.scrollIntoView();
                }
            }
            context.select(context.store.getIdentity(item));
        });
    },

    getSelected: function (store) {
        if (!store) {
            store = this.store;
        }
        var retVal = [];
        for (var id in this.selection) {
            if (this.selection[id]) {
                const rowItem = this.row(id);
                const storeItem = store.get(id);
                if (rowItem && rowItem.data && storeItem && storeItem.StateID !== 999) {
                    retVal.push(storeItem);
                }
            }
        }
        return retVal;
    }
});

export var LocalStorage = dojo.declare([Evented], {
    constructor: function () {
        var context = this;
        if (typeof Storage !== void (0)) {
            window.addEventListener('storage', function (event) {
                context.emit('storageUpdate', { event });
            });
        } else {
            console.log("Browser doesn't support multi-tab communication");
        };
    },

    setItem: function (key, value) {
        localStorage.setItem(key, value);
    },
    removeItem: function (key) {
        localStorage.removeItem(key);
    },
    getItem: function (key, type) {
        return type === "Number" ? Number(localStorage.getItem(key)) : localStorage.getItem(key);
    },
    clear: function () {
        localStorage.clear();
    }
});

export function goToPageUserPreference(gridName, key) {
    var context = this;
    var initUserPref = this.LocalStorage.prototype.getItem(key, "Number");
    if (initUserPref) {
        gridName.rowsPerPage = initUserPref;
        gridName._updateRowsPerPageOption();
    }
    aspect.after(gridName, 'gotoPage', function (deferred, args) {
        return deferred.then(function () {
            var currentUserPref = context.LocalStorage.prototype.getItem(key, "Number");
            var currentGridValue = gridName.rowsPerPage;
            if (currentUserPref !== currentGridValue) {
                context.LocalStorage.prototype.setItem(key, currentGridValue);
            }
        });
    });
}

export var MonitorLockClick = dojo.declare([Evented], {
    unlocked: function () {
        this.emit("unlocked", {});
    },
    locked: function () {
        this.emit("locked", {});
    }
});

export var IdleWatcher = dojo.declare([Evented], {
    constructor: function (idleDuration) {
        idleDuration = idleDuration || 30 * 1000;
        this._idleDuration = idleDuration;
    },

    fireIdle: function () {
        this.emit("idle", { status: "firedIdle" });
    },

    start: function () {
        this.stop();
        var context = this;
        this._keydownHandle = on(document, "keydown", function (item, index, array) {
            context.emit("active", {});
            context.stop();
            context.start();
        });
        this._mousedownHandle = on(document, "mousedown", function (item, index, array) {
            context.emit("active", {});
            context.stop();
            context.start();
        });
        this._intervalHandle = setInterval(function () {
            context.emit("idle", {});
        }, this._idleDuration);
    },

    stop: function () {
        if (this._intervalHandle) {
            clearInterval(this._intervalHandle);
            delete this._intervalHandle;
        }
        if (this._mousedownHandle) {
            this._mousedownHandle.remove();
            delete this._mousedownHandle;
        }
        if (this._keydownHandle) {
            this._keydownHandle.remove();
            delete this._keydownHandle;
        }
    }
});

export const Singleton = SingletonData;

export const FormHelper = declare(null, {
    getISOString: function (dateField, timeField) {
        var d = registry.byId(this.id + dateField).attr("value");
        var t = registry.byId(this.id + timeField).attr("value");
        if (d) {
            if (t) {
                d.setHours(t.getHours() - d.getTimezoneOffset() / 60);
                d.setMinutes(t.getMinutes());
                d.setSeconds(t.getSeconds());
            }
            return d.toISOString();
        }
        return "";
    }
});

type Memory = {
    data: any[];
    queryEngine: any;
    setData(data: any);
    query(query: any, options: any): any;
};

export interface UndefinedMemoryBase extends Memory {
}

@declareDecorator("UndefinedMemoryBase", Memory)
export class UndefinedMemoryBase { }

export class UndefinedMemory extends UndefinedMemoryBase {

    constructor() {
        super();
    }

    query(query: any, options: any) {
        if (options.sort !== undefined) {
            if (options.sort.length > 0) {
                var sortSet = options.sort;
                options.sort = function (a, b) {
                    for (var sort, i = 0; sort = sortSet[i]; i++) {
                        var aValue = a[sort.attribute];
                        var bValue = b[sort.attribute];
                        // valueOf enables proper comparison of dates
                        aValue = aValue != null ? aValue.valueOf() : aValue;
                        bValue = bValue != null ? bValue.valueOf() : bValue;
                        if (aValue != bValue) {
                            if (aValue === undefined) return 1;     // Force "undefined" to bottom always
                            if (bValue === undefined) return -1;    // Force "undefined" to bottom always
                            return !!sort.descending == (aValue == null || aValue > bValue) ? -1 : 1;
                        }
                    }
                    return 0;
                }
            }
        }
        return super.query(query, options);
    }
}

export function Grid(pagination?, selection?, overrides?, compoundColumns?, gridName?) {
    var baseClass = [];
    var params = {};
    var rows =  Number(localStorage.getItem(gridName))

    if (pagination) {
        baseClass = [DGrid, Pagination, ColumnResizer, Keyboard, DijitRegistry, CompoundColumns];
        lang.mixin(params, {
            rowsPerPage: rows || 50,
            pagingLinks: 1,
            pagingTextBox: true,
            firstLastArrows: true,
            pageSizeOptions: [25, 50, 100, 1000]
        });
    } else {
        baseClass = [OnDemandGrid, ColumnResizer, Keyboard, DijitRegistry];
    }
    if (compoundColumns) {
        baseClass.push(CompoundColumns);
    }
    if (selection) {
        baseClass.push(Selection);
        lang.mixin(params, {
            allowSelectAll: true,
            deselectOnRefresh: false
        });
    }
    baseClass.push(GridHelper);
    return declare(baseClass, lang.mixin(params, overrides));
}

export function MonitorVisibility(widget, callback) {
    //  There are many places that may cause the widget to be hidden, the possible places are calculated by walking the DOM hierarchy upwards. 
    var watchList = {};
    var domNode = widget.domNode;
    while (domNode) {
        if (domNode.id) {
            watchList[domNode.id] = false;
        }
        domNode = domNode.parentElement;
    }

    function isHidden() {
        for (var key in watchList) {
            if (watchList[key] === true) {
                return true;
            }
        }
        return false;
    }

    //  Hijack the dojo style class replacement call and monitor for elements in our watchList. 
    aspect.around(domClass, "replace", function (origFunc) {
        return function (node, addStyle, removeStyle) {
            if (node.firstChild && (node.firstChild.id in watchList)) {
                if (addStyle === "dijitHidden" || addStyle === "hpccHidden") {
                    if (!isHidden()) {
                        if (callback(false, node)) {
                            addStyle = "hpccHidden";
                            removeStyle = "hpccVisible";
                        }
                    }
                    watchList[node.firstChild.id] = true;
                } else if ((addStyle === "dijitVisible" || addStyle === "hpccVisible") && watchList[node.firstChild.id] === true) {
                    watchList[node.firstChild.id] = false;
                    if (!isHidden()) {
                        if (callback(true, node)) {
                            addStyle = "hpccVisible";
                            removeStyle = "hpccHidden";
                        }
                    }
                }
            }
            return origFunc(node, addStyle, removeStyle);
        }
    });
}

var slice = Array.prototype.slice;
export function override(method) {
    var proxy;

    /** @this target object */
    proxy = function () {
        var me = this;
        var inherited = (this.getInherited && this.getInherited({
            // emulating empty arguments
            callee: proxy,
            length: 0
        })) || function () { };

        return method.apply(me, [function () {
            return inherited.apply(me, arguments);
        }].concat(slice.apply(arguments)));
    };

    proxy.method = method;
    proxy.overrides = true;

    return proxy;
}

export interface IMaximizeState {
    parentNode: HTMLElement;
    nextElementSibling: HTMLElement;
    stylePosition: string;
    stubResize: (size?: { l: number, t: number, w: number, h: number }) => void;
    widgetResize: (size?: { l: number, t: number, w: number, h: number }) => void;
}
export function maximizeWidget(widget: any, max: boolean, prev?: IMaximizeState): IMaximizeState | undefined {
    var stub = registry.byId("stub");
    if (stub.domNode !== widget.domNode) {
        if (max) {
            var retVal: IMaximizeState = {
                parentNode: widget.domNode.parentNode,
                nextElementSibling: widget.domNode.nextElementSibling,
                stylePosition: domStyle.get(widget.domNode, "position"),
                stubResize: stub.resize,
                widgetResize: widget.resize
            };
            widget.domNode.style.setProperty("position", "relative", "important");

            stub.domNode.insertBefore(widget.domNode, stub.domNode.firstChild);

            stub.resize = function () {
                widget.resize();
            };

            widget.resize = function () {
                retVal.widgetResize.call(widget, { l: 0, t: 0, w: window.innerWidth, h: window.innerHeight });
            };

            widget.resize();

            return retVal;
        } else {
            widget.resize = prev.widgetResize;
            stub.resize = prev.stubResize;

            if (prev.nextElementSibling) {
                prev.parentNode.insertBefore(widget.domNode, prev.nextElementSibling);
            } else {
                prev.parentNode.appendChild(widget.domNode);
            }
            domStyle.set(widget.domNode, "position", prev.stylePosition);

            stub.resize();
        }
    }
}
