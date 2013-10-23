﻿/*##############################################################################
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
    "dojo/_base/lang",
    "dojo/_base/config",
    "dojo/_base/Deferred",
    "dojo/request",
    "dojo/request/script",
    "dojo/store/util/QueryResults",
    "dojo/store/Observable"

], function (declare, arrayUtil, lang, config, Deferred, request, script, QueryResults, Observable) {
    var RequestHelper = declare(null, {

        serverIP: null,

        constructor: function (args) {
            if (args) {
                declare.safeMixin(this, args);
            }
            this.serverIP = this.getParamFromURL("ServerIP");
        },

        getParamFromURL: function (key) {
            var value = "";
            if (dojo.doc.location.search) {
                var searchStr = dojo.doc.location.search.substr((dojo.doc.location.search.substr(0, 1) == "?" ? 1 : 0));
                value = searchStr ? dojo.queryToObject(searchStr)[key] : "";
            }

            if (value)
                return value;
            return config[key];
        },

        getBaseURL: function (service) {
            if (service === undefined) {
                service = "WsWorkunits";
            }
            if (this.serverIP)
                return "http://" + this.serverIP + ":8010/" + service;
            return "/" + service;
        },

        isCrossSite: function () {
            return this.serverIP ? true : false;
        },

        _send: function(service, action, _params) {
            var params = lang.mixin({
                load: function (response) {
                },
                error: function (error) {
                },
                event: function (evt) {
                }
            }, _params);
            lang.mixin(params.request, {
                rawxml_: true
            });

            var handleAs = params.handleAs ? params.handleAs : "json";
            var postfix = "";
            if (handleAs === "json") {
                postfix = ".json";
            }
            var method = params.method ? params.method : "get";

            var retVal = null;
            if (this.isCrossSite()) {
                retVal = script.get(this.getBaseURL(service) + "/" + action + postfix, {
                    query: params.request,
                    jsonp: "jsonp"
                });
            } else {
                retVal = request.post(this.getBaseURL(service) + "/" + action + postfix, {
                    data: params.request,
                    handleAs: handleAs
                });
            }
            return retVal.then(function (response) {
                params.load(response);
                return response;
            },
            function (error) {
                params.error(error);
                return error;
            },
            function (event) {
                params.event(event);
                return event;
            });
        },

        send: function (service, action, params) {
            if (!params)
                params = {};

            dojo.publish("hpcc/standbyBackgroundShow");
            var handleAs = params.handleAs ? params.handleAs : "json";
            return this._send(service, action, params).then(function (response) {
                if (!params.suppressExceptionToaster && handleAs == "json") {
                    var deletedWorkunit = false; //  Deleted WU
                    if (lang.exists("Exceptions.Source", response)) {
                        dojo.publish("hpcc/brToaster", {
                            Severity: "Error",
                            Source: service + "." + action,
                            Exceptions: response.Exceptions
                        });
                    }
                }
                dojo.publish("hpcc/standbyBackgroundHide");
                return response;
            },
            function (error) {
                var message = "Unknown Error";
                if (lang.exists("response.text", error)) {
                    message = error.response.text;
                } else if (error.message && error.stack) {
                    message = "<h3>" + error.message + "</h3>";
                    message += "<p>" + error.stack + "</p>";
                }

                dojo.publish("hpcc/brToaster", {
                    Severity: "Error",
                    Source: service + "." + action,
                    Exceptions: [{ Message: message }]
                });
                dojo.publish("hpcc/standbyBackgroundHide");
                return error;
            });
        },

        //  XML to JSON helpers  ---
        getValue: function (domXml, tagName, knownObjectArrays) {
            var retVal = this.getValues(domXml, tagName, knownObjectArrays);
            if (retVal.length == 0) {
                return null;
            } else if (retVal.length != 1) {
                alert("Invalid length:  " + retVal.length);
            }
            return retVal[0];
        },

        getValues: function (domXml, tagName, knownObjectArrays) {
            var retVal = [];
            var items = domXml.getElementsByTagName(tagName);
            var parentNode = items.length ? items[0].parentNode : null; //  Prevent <Dataset><row><field><row> scenario
            for (var i = 0; i < items.length; ++i) {
                if (items[i].parentNode == parentNode)
                    retVal.push(this.flattenXml(items[i], knownObjectArrays));
            }
            return retVal;
        },

        flattenXml: function (domXml, knownObjectArrays) {
            var retValArr = [];
            var retValStr = "";
            var retVal = {};
            for (var i = 0; i < domXml.childNodes.length; ++i) {
                var childNode = domXml.childNodes[i];
                if (childNode.childNodes) {
                    if (childNode.nodeName && knownObjectArrays != null && dojo.indexOf(knownObjectArrays, childNode.nodeName) >= 0) {
                        retValArr.push(this.flattenXml(childNode, knownObjectArrays));
                    } else if (childNode.nodeName == "#text") {
                        retValStr += childNode.nodeValue;
                    } else if (childNode.childNodes.length == 0) {
                        retVal[childNode.nodeName] = null;
                    } else {
                        var value = this.flattenXml(childNode, knownObjectArrays);
                        if (retVal[childNode.nodeName] == null) {
                            retVal[childNode.nodeName] = value;
                        } else if (dojo.isArray(retVal[childNode.nodeName])) {
                            retVal[childNode.nodeName].push(value);
                        } else if (dojo.isObject(retVal[childNode.nodeName])) {
                            var tmp = retVal[childNode.nodeName];
                            retVal[childNode.nodeName] = [];
                            retVal[childNode.nodeName].push(tmp);
                            retVal[childNode.nodeName].push(value);
                        }
                    }
                }
            }
            if (retValArr.length)
                return retValArr;
            else if (retValStr.length)
                return retValStr;
            return retVal;
        }
    });

    _StoreSingletons = [];
    return {
        flattenArray: function (target, arrayName, arrayID) {
            if (lang.exists(arrayName + ".length", target)) {
                var tmp = {};
                for (var i = 0; i < target[arrayName].length; ++i) {
                    tmp[arrayName + "_i" + i] = target[arrayName][i][arrayID];
                }
                delete target[arrayName];
                return lang.mixin(target, tmp);
            }
            return target;
        },

        flattenMap: function (target, arrayName) {
            if (lang.exists(arrayName, target)) {
                var appData = target[arrayName];
                delete target[arrayName];
                var singularName = arrayName.substr(0, arrayName.length - 1);
                var i = 0;
                for (var key in appData) {
                    target[arrayName + "." + singularName + "." + i + '.Application'] = "ESPRequest.js";
                    target[arrayName + "." + singularName + "." + i + '.Name'] = key;
                    target[arrayName + "." + singularName + "." + i + '.Value'] = appData[key];
                    ++i;
                }
                target[arrayName + "." + singularName + ".itemcount"] = i;
            }
            return target;
        },

        getBaseURL: function (service) {
            var helper = new RequestHelper();
            return helper.getBaseURL(service);
        },

        send: function (service, action, params) {
            var helper = new RequestHelper();
            return helper.send(service, action, params);
        },

        Store: declare(null, {
            SortbyProperty: 'Sortby',
            DescendingProperty: 'Descending',

            constructor: function (options) {
                if (!this.service) {
                    throw new Error("service:  Undefined - Missing service name (eg 'WsWorkunts').");
                }
                if (!this.action) {
                    throw new Error("action:  Undefined - Missing action name (eg 'WUQuery').");
                }
                if (!this.responseQualifier) {
                    throw new Error("responseQualifier:  Undefined - Missing action name (eg 'Workunits.ECLWorkunit').");
                }
                if (!this.idProperty) {
                    throw new Error("idProperty:  Undefined - Missing ID field (eg 'Wuid').");
                }
                if (options) {
                    declare.safeMixin(this, options);
                }
            },

            getIdentity: function (item) {
                return item[this.idProperty];
            },

            exists: function (id) {
                var item = lang.getObject(this.service + "." + this.action, false, _StoreSingletons);
                if (item) {
                    return item[id] !== undefined;
                }
                return false;
            },

            get: function (id, item) {
                if (!this.exists(id)) {
                    var newItem = lang.getObject(this.service + "." + this.action, true, _StoreSingletons);
                    newItem[id] = this.create(id, item);
                    return newItem[id];
                }
                var item = lang.getObject(this.service + "." + this.action, false, _StoreSingletons);
                return item[id];
            },

            create: function (id, item) {
                var retVal = {
                };
                retVal[this.idProperty] = id;
                return retVal;
            },

            update: function (id, item) {
                lang.mixin(this.get(id), item);
            },

            _hasResponseContent: function(response) {
                return lang.exists(this.responseQualifier, response);
            },

            _getResponseContent: function(response) {
                return lang.getObject(this.responseQualifier, false, response);
            },

            query: function (query, options) {
                var request = query;
                if (options !== undefined && options.start !== undefined && options.count !== undefined) {
                    if (this.startProperty) {
                        request[this.startProperty] = options.start;
                    }
                    if (this.countProperty) {
                        request[this.countProperty] = options.count;
                    }
                }
                if (options !== undefined && options.sort !== undefined && options.sort[0].attribute !== undefined) {
                    request[this.SortbyProperty] = options.sort[0].attribute;
                    request[this.DescendingProperty] = options.sort[0].descending ? true : false;
                }
                if (this.preRequest) {
                    this.preRequest(request);
                }
                var helper = new RequestHelper();
                var results = helper.send(this.service, this.action, {
                    request: request
                });

                var deferredResults = new Deferred();
                var context = this;
                deferredResults.total = results.then(function (response) {
                    if (context.responseTotalQualifier) {
                        return lang.getObject(context.responseTotalQualifier, false, response);
                    } else if (context._hasResponseContent(response)) {
                        return context._getResponseContent(response).length;
                    }
                    return 0;
                });
                Deferred.when(results, function (response) {
                    var items = [];
                    if (context._hasResponseContent(response)) {
                        if (context.preProcessFullResponse) {
                            context.preProcessFullResponse(response, request, query, options);
                        }
                        if (context.preProcessResponse) {
                            var responseQualiferArray = context.responseQualifier.split(".");
                            context.preProcessResponse(lang.getObject(responseQualiferArray[0], false, response), request, query, options);
                        }
                        arrayUtil.forEach(context._getResponseContent(response), function (item, index) {
                            if (context.preProcessRow) {
                                context.preProcessRow(item, request, query, options);
                            }
                            var storeItem = context.get(context.getIdentity(item), item);
                            context.update(context.getIdentity(item), item);
                            items.push(storeItem);
                        });
                    }
                    if (context.postProcessResults) {
                        context.postProcessResults(items);
                    }
                    deferredResults.resolve(items);
                    return items;
                });

                return QueryResults(deferredResults);
            }
        })
    };
});
