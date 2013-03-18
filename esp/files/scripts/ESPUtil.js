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
    "dojo/Stateful"
], function (declare, Stateful) {

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
            return {};
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
        startMonitor: function () {
            this._timerTickCount = 0;
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
        Monitor: Monitor
    };
});
