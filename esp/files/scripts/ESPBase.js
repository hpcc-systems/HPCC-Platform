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
    "dojo/_base/config"
], function (declare, config) {
    return declare(null, {

        constructor: function (args) {
            declare.safeMixin(this, args);
        },

        getParam: function (key) {
            var value = dojo.queryToObject(dojo.doc.location.search.substr((dojo.doc.location.search.substr(0, 1) == "?" ? 1 : 0)))[key];

            if (value)
                return value;
            return config[key];
        },

        getBaseURL: function (service) {
            if (!service) {
                service = "WsWorkunits";
            }
            var serverIP = this.getParam("serverIP");
            if (serverIP)
                return "http://" + serverIP + ":8010/" + service;
            return "/" + service;
        },

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
});
