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
    "dojo/_base/array",
    "dojo/Stateful",

    "dijit/registry",
], function (declare, arrayUtil, Stateful,
    registry) {

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
                var json = dojo.toJson(response[key]);
                if (this._changedCache[key] !== json) {
                    this._changedCache[key] = json;
                    this.set(key, response[key]);
                    changed = true;
                }
            }
            if (changed) {
                this.set("changedCount", this.get("changedCount") + 1);
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
            workunitsGridObserver: [],

            onSelectionChanged: function (callback) {
                this.on("dgrid-select", function (event) {
                    callback(event);
                });
                this.on("dgrid-deselect", function (event) {
                    callback(event);
                });
            },

            onContentChanged: function (callback) {
                var context = this;
                this.on("dgrid-page-complete", function (event) {
                    callback();
                    if (context.workunitsGridObserver[event.page]) {
                        context.workunitsGridObserver[event.page].cancel();
                    }
                    context.workunitsGridObserver[event.page] = event.results.observe(function (object, removedFrom, insertedInto) {
                        callback(object, removedFrom, insertedInto);
                    }, true);
                });
            },

            setSelection: function (arrayOfIDs) {
                this.clearSelection();
                var context = this;
                arrayUtil.forEach(arrayOfIDs, function (item, idx) {
                    context.select(item);
                });
            },

            setSelected: function (items) {
                this.clearSelection();
                var context = this;
                arrayUtil.forEach(items, function (item, idx) {
                    context.select(context.store.getIdentity(item));
                });
            },

            getSelected: function() {
                var retVal = [];
                for (var id in this.selection) {
                    retVal.push(this.store.get(id));
                }
                return retVal;
            }
        })
    };
});
