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
    "dojo/_base/xhr",
    "hpcc/ESPResult",
    "hpcc/ESPBase"
], function (declare, lang, xhr, ESPResult, ESPBase) {
    return declare(ESPBase, {
        Wuid: "",

        stateID: 0,
        state: "",

        text: "",

        resultCount: 0,
        results: [],

        graphs: [],

        exceptions: [],
        timers: [],

        onCreate: function () {
        },
        onUpdate: function () {
        },
        onSubmit: function () {
        },
        constructor: function (args) {
            declare.safeMixin(this, args);
        },
        isComplete: function () {
            switch (this.stateID) {
                case 6:	//DFUStateFinished:
                    return true;
            }
            return false;
        },
        monitor: function (callback, monitorDuration) {
            if (!monitorDuration)
                monitorDuration = 0;
            var request = {
            		wuid: this.Wuid
            };
            //	request['rawxml_'] = "1";

            var context = this;
            xhr.post({
                url: this.getBaseURL("FileSpray") + "/GetDFUWorkunit.json",
                handleAs: "json",
                content: request,
                load: function (response) {
                	var workunit = response.GetDFUWorkunitResponse.result;
                    context.stateID = workunit.State;
                    context.state = workunit.StateMessage;
                    if (callback) {
                        callback(workunit);
                    }

                    if (!context.isComplete()) {
                        var timeout = 30;	// Seconds

                        if (monitorDuration < 5) {
                            timeout = 1;
                        } else if (monitorDuration < 10) {
                            timeout = 2;
                        } else if (monitorDuration < 30) {
                            timeout = 5;
                        } else if (monitorDuration < 60) {
                            timeout = 10;
                        } else if (monitorDuration < 120) {
                            timeout = 20;
                        }
                        setTimeout(function () {
                            context.monitor(callback, monitorDuration + timeout);
                        }, timeout * 1000);
                    }
                },
                error: function () {
                    done = true;
                }
            });
        },
        fetchStructure: function (format, onFetchStructure) {
            var request = {
                Name: this.logicalName,
                Format: format,
                rawxml_: true
            };

            var context = this;
            xhr.post({
                url: this.getBaseURL("FileSpray") + "/DFUWUFile",
                handleAs: "text",
                content: request,
                load: function (response) {
                    onFetchStructure(response);
                },
                error: function (e) {
                }
            });
        },
        
        fetchDEF: function (onFetchXML) {
            this.fetchStructure("def", onFetchXML);
        },
        
        fetchXML: function (onFetchXML) {
            var request = {                
                Wuid: this.Wuid
            };

            var context = this;
            xhr.post({
                url: this.getBaseURL("FileSpray") + "/DFUWUFile",
                handleAs: "text",
                content: request,
                load: function (response) {
                    onFetchXML(response);
                },
                error: function (e) {
                    var d = 0;
                }
            });
        },

        create: function (ecl) {
        },
        
        update: function (request, appData, callback) {
        },
        
        submit: function (target) {
        },
        
        _resubmit: function (clone, resetWorkflow, callback) {
        },
        
        clone: function (callback) {
            this._resubmit(true, false, callback);
        },
        
        resubmit: function (callback) {
            this._resubmit(false, false, callback);
        },
        
        restart: function (callback) {
            this._resubmit(false, true, callback);
        },
        
        _action: function (action, callback) {
        },
        
        abort: function (callback) {
            this._action("Abort", callback);
        },
        
        doDelete: function (callback) {
            this._action("Delete", callback);
        },
        
        getInfo: function (args) {
            var request = {
            	wuid: this.Wuid
            };

            var context = this;
            xhr.post({
                url: this.getBaseURL("FileSpray") + "/GetDFUWorkunit.json",
                handleAs: "json",
                content: request,
                load: function (response) {
                	var workunit = response.GetDFUWorkunitResponse.result;
                    context.GetDFUWorkunitResponse = workunit;

                    if (args.onGetAll) {
                        args.onGetAll(workunit);
                    }
                },
                error: function (e) {
                }
            });
        },
        getState: function () {
            return this.state;
        },
        getProtectedImage: function () {
            if (this.protected) {
                return "img/locked.png"
            }
            return "img/unlocked.png"
        },
        getStateImage: function () {
            switch (this.stateID) {
                case 6:
                    return "img/workunit_completed.png";
            }
            return "img/workunit.png";
        }
    });
});
