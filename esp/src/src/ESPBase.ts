import * as config from "dojo/_base/config";
import * as declare from "dojo/_base/declare";

declare const dojo;

export class ESPBase {

    constructor(args?) {
        if (args) {
            declare.safeMixin(this, args);
        }
    }

    getParam(key) {
        const value = dojo.queryToObject(dojo.doc.location.search.substr((dojo.doc.location.search.substr(0, 1) === "?" ? 1 : 0)))[key];

        if (value)
            return value;
        return config[key];
    }

    getBaseURL(service) {
        if (!service) {
            service = "WsWorkunits";
        }
        const serverIP = this.getParam("serverIP");
        if (serverIP)
            return "http://" + serverIP + ":8010/" + service;
        return "/" + service;
    }

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
