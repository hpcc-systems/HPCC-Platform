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
    "dojo/store/Observable",
    "dojo/topic"

], function (declare, arrayUtil, lang, config, Deferred, request, script, QueryResults, Observable, topic) {
    var RequestHelper = declare(null, {

        serverIP: null,

        constructor: function (args) {
            if (args) {
                declare.safeMixin(this, args);
            }
            this.serverIP = (typeof debugConfig !== "undefined") ? debugConfig.IP : this.getParamFromURL("ServerIP");
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

            var handleAs = params.handleAs ? params.handleAs : "json";
            return this._send(service, action, params).then(function (response) {
                if (handleAs === "json") {
                    if (lang.exists("Exceptions.Source", response)) {
                        var severity = params.suppressExceptionToaster ? "Info" : "Error";
                        var source = service + "." + action;
                        if (lang.exists("Exceptions.Exception", response) && response.Exceptions.Exception.length === 1) {
                            switch (source) {
                                case "WsWorkunits.WUInfo":
                                    if (response.Exceptions.Exception[0].Code === 20080) {
                                        severity = "Info";
                                    }
                                    break;
                                case "WsWorkunits.WUQuery":
                                    if (response.Exceptions.Exception[0].Code === 20081) {
                                        severity = "Info";
                                    }
                                    break;
                                case "FileSpray.GetDFUWorkunit":
                                    if (response.Exceptions.Exception[0].Code === 20080) {
                                        severity = "Info";
                                    }
                                    break;
                                case "WsDfu.DFUInfo":
                                    if (response.Exceptions.Exception[0].Code === 20038) {
                                        severity = "Info";
                                    }
                                    break;
                            }
                        }
                        topic.publish("hpcc/brToaster", {
                            Severity: severity,
                            Source: source,
                            Exceptions: response.Exceptions.Exception
                        });
                    }
                }
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

                topic.publish("hpcc/brToaster", {
                    Severity: "Error",
                    Source: service + "." + action,
                    Exceptions: [{ Message: message }]
                });
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
        getURL: function (_params) {
            var requestHelper = new RequestHelper();
            var params = lang.mixin({
                protocol: location.protocol,
                hostname: requestHelper.serverIP ? requestHelper.serverIP : location.hostname,
                port: location.port,
                pathname: ""
            }, _params);
            return params.protocol + "//" + params.hostname + ":" + params.port + params.pathname;
        },

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

        flattenMap: function (target, arrayName, _singularName, supressAppName, excludeEmptyValues) {
            if (lang.exists(arrayName, target)) {
                var appData = target[arrayName];
                delete target[arrayName];
                var singularName = _singularName ? _singularName : arrayName.substr(0, arrayName.length - 1);
                var i = 0;
                for (var key in appData) {
                    if (excludeEmptyValues && (!appData[key] || appData[key] === "")) {
                        continue;
                    }
                    if (!supressAppName) {
                        target[arrayName + "." + singularName + "." + i + '.Application'] = "ESPRequest.js";
                    }
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
            useSingletons: true,

            constructor: function (options) {
                this.cachedArray = {};

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

            endsWith: function (str, suffix) {
                return str.indexOf(suffix, str.length - suffix.length) !== -1;
            },

            getIdentity: function (item) {
                return item[this.idProperty];
            },

            getCachedArray: function (create) {
                return this.useSingletons ? lang.getObject(this.service + "." + this.action, create, _StoreSingletons) : this.cachedArray;
            },

            exists: function (id) {
                var cachedArray = this.getCachedArray(false);
                if (cachedArray) {
                    return cachedArray[id] !== undefined;
                }
                return false;
            },

            get: function (id, item) {
                if (!this.exists(id)) {
                    var cachedArray = this.getCachedArray(true);
                    cachedArray[id] = this.create(id, item);
                    return cachedArray[id];
                }
                var cachedArray = this.getCachedArray(false);
                return cachedArray[id];
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
                var deferredResults = new Deferred();
                deferredResults.total = new Deferred();
                var helper = new RequestHelper();
                var context = this;
                var results = helper.send(this.service, this.action, {
                    request: request
                }).then(function(response) {
                    if (context.preProcessFullResponse) {
                        context.preProcessFullResponse(response, request, query, options);
                    }
                    var items = [];
                    if (context._hasResponseContent(response)) {
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
                    if (context.responseTotalQualifier) {
                        deferredResults.total.resolve(lang.getObject(context.responseTotalQualifier, false, response));
                    } else if (context._hasResponseContent(response)) {
                        deferredResults.total.resolve(items.length);
                    } else {
                        deferredResults.total.resolve(0);
                    }
                    deferredResults.resolve(items);
                    return response;
                });
                return QueryResults(deferredResults);
            }
        })
    };
});
