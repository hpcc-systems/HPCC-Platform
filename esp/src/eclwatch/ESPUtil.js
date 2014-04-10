/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/Stateful",
    "dojo/json",

    "dijit/registry",
    "dijit/Tooltip"
], function (declare, lang, i18n, nlsHPCC, arrayUtil, Stateful, json,
    registry, Tooltip) {

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
            this.changedCount = 0;
            this._changedCache = {};
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
                var jsonStr = json.stringify(response[key]);
                if (this._changedCache[key] !== jsonStr) {
                    this._changedCache[key] = jsonStr;
                    this.set(key, response[key]);
                    changed = true;
                }
            }
            if (changed) {
                try {
                    this.set("changedCount", this.get("changedCount") + 1);
                } catch (e) {
                    /*  changedCount can notify a dgrid instance that a row has changed.  
                    *   There is an issue (TODO check issue number) with dgrid which can cause an exception to be thrown during the notify.
                    *   By catching these exceptions here normal execution can continue.
                    */
                }
            }
        }
    });

    var Monitor = declare(null, {
        isMonitoring: function () {
            return this._timer && this._timer > 0;
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

    return {
        Singleton: SingletonData,
        Monitor: Monitor,

        FormHelper: declare(null, {
            getISOString: function (dateField, timeField) {
                var d = registry.byId(this.id + dateField).attr("value");
                var t = registry.byId(this.id + timeField).attr("value");
                if (d) {
                    if (t) {
                        d.setHours(t.getHours());
                        d.setMinutes(t.getMinutes());
                        d.setSeconds(t.getSeconds());
                    }
                    return d.toISOString();
                }
                return "";
            }
        }),

        GridHelper: declare(null, {
            allowTextSelection: true,
            __hpcc_pagedGridObserver: [],
            noDataMessage: "<span class='dojoxGridNoData'>" + nlsHPCC.noDataMessage + "</span>",
            loadingMessage: "<span class='dojoxGridNoData'>" + nlsHPCC.loadingMessage + "</span>",

            postCreate: function (args) {
                this.inherited(arguments);

                this.__hpcc_tooltip = new Tooltip({
                    connectId: [this.id],
                    selector: "td",
                    showDelay: 800,
                    getContent: function (node) {
                        if (node.offsetWidth < node.scrollWidth) {
                            return node.innerHTML;
                        }
                        return "";
                    }
                });
            },

            _onNotify: function(object, existingId){
                this.inherited(arguments);
                if (this.onSelectedChangedCallback) {
                    this.onSelectedChangedCallback();
                }
            },

            onSelectionChanged: function (callback) {
                this.onSelectedChangedCallback = callback;
                this.on("dgrid-select, dgrid-deselect", function (event) {
                    callback(event);
                });
            },

            onContentChanged: function (callback) {
                var context = this;
                this.on("dgrid-page-complete", function (event) {
                    callback();
                    if (context.__hpcc_pagedGridObserver[event.page]) {
                        context.__hpcc_pagedGridObserver[event.page].cancel();
                    }
                    context.__hpcc_pagedGridObserver[event.page] = event.results.observe(function (object, removedFrom, insertedInto) {
                        callback(object, removedFrom, insertedInto);
                    }, true);
                });
                this.on("dgrid-children-complete", function (event) {
                    callback();
                });
            },

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

            getSelected: function(store) {
                if (!store) {
                    store = this.store;
                }
                var retVal = [];
                for (var id in this.selection) {
                    if (this.selection[id]) {
                        var storeItem = store.get(id);
                        if (storeItem) {
                            retVal.push(storeItem);
                        }
                    }
                }
                return retVal;
            }
        })
    };
});
