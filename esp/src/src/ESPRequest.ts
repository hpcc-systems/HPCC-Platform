import * as arrayUtil from "dojo/_base/array";
import * as config from "dojo/_base/config";
import * as declare from "dojo/_base/declare";
import * as Deferred from "dojo/_base/Deferred";
import * as lang from "dojo/_base/lang";
import * as cookie from "dojo/cookie";
import * as QueryResults from "dojo/store/util/QueryResults";
import * as topic from "dojo/topic";
import * as ESPUtil from "./ESPUtil";

import * as hpccComms from "@hpcc-js/comms";

declare const dojo: any;
declare const debugConfig: any;

class RequestHelper {

    serverIP: null;
    timeOutSeconds: number;

    constructor() {
        this.serverIP = (typeof debugConfig !== "undefined") ? debugConfig.IP : this.getParamFromURL("ServerIP");
        this.timeOutSeconds = 60;
    }

    getParamFromURL(key) {
        let value = "";
        if (dojo.doc.location.search) {
            const searchStr = dojo.doc.location.search.substr((dojo.doc.location.search.substr(0, 1) === "?" ? 1 : 0));
            value = searchStr ? dojo.queryToObject(searchStr)[key] : "";
        }

        if (value)
            return value;
        return config[key];
    }

    getBaseURL(service) {
        if (service === undefined) {
            service = "WsWorkunits";
        }
        if (this.serverIP)
            return "http://" + this.serverIP + ":8010/" + service;
        return "/" + service;
    }

    isCrossSite() {
        return this.serverIP ? true : false;
    }

    isSessionCall(service, action) {
        switch (service) {
            case "esp":
                switch (action) {
                    // case "login":
                    // case "logout":
                    // case "lock":
                    case "unlock":
                        return true;
                }
                break;
        }
        return false;
    }

    hasServerSetCookie() {
        return cookie("ESPSessionState") !== undefined;
    }

    hasAuthentication() {
        const retVal = cookie("ESPSessionState");
        return retVal === "true" || retVal === true;
    }

    isAuthenticated() {
        const retVal = cookie("ESPAuthenticated");
        return retVal === "true" || retVal === true;
    }

    isLocked() {
        return cookie("Status") === "Locked";
    }

    _send(service, action, _params) {
        const params = lang.mixin({
            request: {},
            load(response) {
            },
            error(error) {
            },
            event(evt) {
            }
        }, _params);
        lang.mixin(params.request, {
            rawxml_: true
        });

        const handleAs = params.handleAs ? params.handleAs : "json";
        let postfix = "";
        if (handleAs === "json") {
            postfix = ".json";
        }
        // var method = params.method ? params.method : "get";

        let retVal = null;
        if (this.isCrossSite()) {
            const transport = new hpccComms.Connection({ baseUrl: this.getBaseURL(service), timeoutSecs: params.request.timeOutSeconds || this.timeOutSeconds, type: "jsonp" });
            retVal = transport.send(action + postfix, params.request, handleAs === "text" ? "text" : "json");
        } else {
            const transport = new hpccComms.Connection({ baseUrl: this.getBaseURL(service), timeoutSecs: params.request.timeOutSeconds || this.timeOutSeconds });
            retVal = transport.send(action + postfix, params.request, handleAs === "text" ? "text" : "json");
        }

        return retVal.then(function (response) {
            if (lang.exists("Exceptions.Exception", response)) {
                if (response.Exceptions.Exception.Code === "401") {
                    if (cookie("Status") === "Unlocked") {
                        topic.publish("hpcc/session_management_status", {
                            status: "DoIdle"
                        });
                    }
                    cookie("Status", "Locked");
                    ESPUtil.LocalStorage.removeItem("Status");
                }
            }
            params.load(response);
            return response;
        }).catch(function (error) {
            params.error(error);
            return error;
        });
    }

    send(service, action, params?) {
        if (!this.isSessionCall(service, action) && (!this.hasServerSetCookie() || (this.hasAuthentication() && !this.isAuthenticated()))) {
            // tslint:disable-next-line: deprecation
            window.location.reload(true);
            return new Promise((resolve, reject) => { });
        }

        if (this.isLocked()) {
            topic.publish("hpcc/brToaster", {
                Severity: "Error",
                Source: service + "." + action,
                Exceptions: [{ Message: "<h3>Session is Locked<h3>" }]
            });
            return Promise.resolve({});
        }

        if (!params)
            params = {};

        const handleAs = params.handleAs ? params.handleAs : "json";
        return this._send(service, action, params).then(function (response) {
            if (handleAs === "json") {
                if (lang.exists("Exceptions.Source", response) && !params.skipExceptions) {
                    let severity = params.suppressExceptionToaster ? "Info" : "Error";
                    const source = service + "." + action;
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
                            case "WsWorkunits.WUCDebug":
                                if (response.Exceptions.Exception[0].Code === -10) {
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
                            case "WsWorkunits.WUUpdate":
                                if (response.Exceptions.Exception[0].Code === 20049) {
                                    severity = "Error";
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
                let message = "Unknown Error";
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
    }

    //  XML to JSON helpers  ---
    getValue(domXml, tagName, knownObjectArrays) {
        const retVal = this.getValues(domXml, tagName, knownObjectArrays);
        if (retVal.length === 0) {
            return null;
        } else if (retVal.length !== 1) {
            alert("Invalid length:  " + retVal.length);
        }
        return retVal[0];
    }

    getValues(domXml, tagName, knownObjectArrays) {
        const retVal = [];
        const items = domXml.getElementsByTagName(tagName);
        const parentNode = items.length ? items[0].parentNode : null; //  Prevent <Dataset><row><field><row> scenario
        for (let i = 0; i < items.length; ++i) {
            if (items[i].parentNode === parentNode)
                retVal.push(this.flattenXml(items[i], knownObjectArrays));
        }
        return retVal;
    }

    flattenXml(domXml, knownObjectArrays) {
        const retValArr = [];
        let retValStr = "";
        const retVal = {};
        for (let i = 0; i < domXml.childNodes.length; ++i) {
            const childNode = domXml.childNodes[i];
            if (childNode.childNodes) {
                if (childNode.nodeName && knownObjectArrays != null && dojo.indexOf(knownObjectArrays, childNode.nodeName) >= 0) {
                    retValArr.push(this.flattenXml(childNode, knownObjectArrays));
                } else if (childNode.nodeName === "#text") {
                    retValStr += childNode.nodeValue;
                } else if (childNode.childNodes.length === 0) {
                    retVal[childNode.nodeName] = null;
                } else {
                    const value = this.flattenXml(childNode, knownObjectArrays);
                    if (retVal[childNode.nodeName] == null) {
                        retVal[childNode.nodeName] = value;
                    } else if (dojo.isArray(retVal[childNode.nodeName])) {
                        retVal[childNode.nodeName].push(value);
                    } else if (dojo.isObject(retVal[childNode.nodeName])) {
                        const tmp = retVal[childNode.nodeName];
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
}

const _StoreSingletons = [];
export function getURL(_params) {
    const requestHelper = new RequestHelper();
    const params = lang.mixin({
        protocol: location.protocol,
        hostname: requestHelper.serverIP ? requestHelper.serverIP : location.hostname,
        port: location.port,
        pathname: ""
    }, _params);
    return params.protocol + "//" + params.hostname + ":" + params.port + params.pathname;
}

export function flattenArray(target, arrayName, arrayID) {
    if (lang.exists(arrayName + ".length", target)) {
        const tmp = {};
        for (let i = 0; i < target[arrayName].length; ++i) {
            tmp[arrayName + "_i" + i] = target[arrayName][i][arrayID];
        }
        delete target[arrayName];
        return lang.mixin(target, tmp);
    }
    return target;
}

export function flattenMap(target, arrayName, _singularName?, supressAppName?, excludeEmptyValues?) {
    if (lang.exists(arrayName, target)) {
        const appData = target[arrayName];
        delete target[arrayName];
        const singularName = _singularName ? _singularName : arrayName.substr(0, arrayName.length - 1);
        let i = 0;
        for (const key in appData) {
            if (excludeEmptyValues && (!appData[key] || appData[key] === "")) {
                continue;
            }
            if (!supressAppName) {
                target[arrayName + "." + singularName + "." + i + ".Application"] = "ESPRequest.js";
            }
            target[arrayName + "." + singularName + "." + i + ".Name"] = key;
            target[arrayName + "." + singularName + "." + i + ".Value"] = appData[key];
            ++i;
        }
        target[arrayName + "." + singularName + ".itemcount"] = i;
    }
    return target;
}

export function getBaseURL(service) {
    const helper = new RequestHelper();
    return helper.getBaseURL(service);
}

export function send(service, action, params?) {
    const helper = new RequestHelper();
    return helper.send(service, action, params);
}

export const Store = declare(null, {
    SortbyProperty: "Sortby",
    DescendingProperty: "Descending",
    useSingletons: true,

    constructor(options) {
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

    endsWith(str, suffix) {
        return str.indexOf(suffix, str.length - suffix.length) !== -1;
    },

    getIdentity(item) {
        return item[this.idProperty];
    },

    getCachedArray(create) {
        return this.useSingletons ? lang.getObject(this.service + "." + this.action, create, _StoreSingletons) : this.cachedArray;
    },

    exists(id) {
        const cachedArray = this.getCachedArray(false);
        if (cachedArray) {
            return cachedArray[id] !== undefined;
        }
        return false;
    },

    get(id, item) {
        if (!this.exists(id)) {
            const cachedArray = this.getCachedArray(true);
            cachedArray[id] = this.create(id, item);
            return cachedArray[id];
        }
        const cachedArray = this.getCachedArray(false);
        return cachedArray[id];
    },

    create(id, item) {
        const retVal = {
        };
        retVal[this.idProperty] = id;
        return retVal;
    },

    update(id, item) {
        lang.mixin(this.get(id), item);
    },

    remove(id) {
        const cachedArray = this.getCachedArray(false);
        if (cachedArray) {
            delete cachedArray[id];
        }
    },

    _hasResponseContent(response) {
        return lang.exists(this.responseQualifier, response);
    },

    _getResponseContent(response) {
        return lang.getObject(this.responseQualifier, false, response);
    },

    query(query, options) {
        const request = query;
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
        const deferredResults = new Deferred();
        deferredResults.total = new Deferred();
        const helper = new RequestHelper();
        const context = this;
        helper.send(this.service, this.action, {
            request
        }).then(function (response) {
            if (context.preProcessFullResponse) {
                context.preProcessFullResponse(response, request, query, options);
            }
            const items = [];
            if (context._hasResponseContent(response)) {
                if (context.preProcessResponse) {
                    const responseQualiferArray = context.responseQualifier.split(".");
                    context.preProcessResponse(lang.getObject(responseQualiferArray[0], false, response), request, query, options);
                }
                arrayUtil.forEach(context._getResponseContent(response), function (item, index) {
                    if (context.preProcessRow) {
                        context.preProcessRow(item, request, query, options);
                    }
                    const storeItem = context.get(context.getIdentity(item), item);
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
});
