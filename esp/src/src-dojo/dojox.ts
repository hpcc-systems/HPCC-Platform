/* 
import * as _debugDojoX from "dojox/main";
const _dojox_keys = Object.keys(_debugDojoX).sort();
if (
    _dojox_keys.join(", ") !==
    "_scopeName, html, xml"
) {
    console.error("DojoX export mismatch", _dojox_keys.join(", "));
}
/* */

export { xml } from "dojox/main";
export * as dojoxXmlParser from "dojox/xml/parser";
export * as dojoxHtmlEntities from "dojox/html/entities";
