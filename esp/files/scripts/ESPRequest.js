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
    "dojo/_base/config",
    "dojo/request",
    "dojo/request/script"
], function (declare, lang, config, request, script) {
    var RequestHelper = declare(null, {

        serverIP: null,

        constructor: function (args) {
            declare.safeMixin(this, args);
            this.serverIP = this.getParamFromURL("ServerIP");
        },

        getParamFromURL: function (key) {
            var value = dojo.queryToObject(dojo.doc.location.search.substr((dojo.doc.location.search.substr(0, 1) == "?" ? 1 : 0)))[key];

            if (value)
                return value;
            return config[key];
        },

        getBaseURL: function (service) {
            if (!service) {
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
            var method = params.method ? params.method : "get";

            var retVal = null;
            if (this.isCrossSite()) {
                retVal = script.get(this.getBaseURL(service) + "/" + action + ".json", {
                    query: params.request,
                    jsonp: "jsonp"
                });
            } else {
                retVal = request.post(this.getBaseURL(service) + "/" + action + ".json", {
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
            dojo.publish("hpcc/standbyBackgroundShow");
            var handleAs = params.handleAs ? params.handleAs : "json";
            return this._send(service, action, params).then(function (response) {
                if (handleAs == "json") {
                    if (lang.exists("Exceptions.Source", response)) {
                        var message = "<h3>" + response.Exceptions.Source + "</h3>";
                        if (lang.exists("Exceptions.Exception", response)) {
                            exceptions = response.Exceptions.Exception;
                            for (var i = 0; i < response.Exceptions.Exception.length; ++i) {
                                message += "<p>" + response.Exceptions.Exception[i].Message + "</p>";
                            }
                        }
                        dojo.publish("hpcc/brToaster", {
                            message: message,
                            type: "error",
                            duration: -1
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
                    message: message,
                    type: "error",
                    duration: -1
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

    return {
        flattenArray: function (target, arrayName) {
            if (lang.exists(arrayName + ".length", target)) {
                var tmp = {};
                for (var i = 0; i < target[arrayName].length; ++i) {
                    tmp[arrayName + "_i" + i] = target[arrayName][i].Wuid;
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
                for (key in appData) {
                    target[arrayName + "." + singularName + "." + i + '.Application'] = "ESPRequest.js";
                    target[arrayName + "." + singularName + "." + i + '.Name'] = key;
                    target[arrayName + "." + singularName + "." + i + '.Value'] = appData[key];
                    ++i;
                }
                target[arrayName + "." + singularName + ".itemcount"] = i;
            }
            return target;
        },

        send: function (service, action, params) {
            var helper = new RequestHelper();
            return helper.send(service, action, params);
        }
    };
});
