/* 
import * as _debug from "dojo/main";
const _debug_keys = Object.keys(_debug).sort();
if (
    _debug_keys.join(", ") !==
    "Animation, Color, Deferred, NodeList, Stateful, _Animation, _Line, _Url, __toPixelValue, _blockAsync, _contentHandlers, _defaultEasing, _destroyElement, _docScroll, _escapeString, _extraNames, _fade, _filterQueryResult, _fixIeBiDiScrollLeft, _getBorderExtents, _getContentBox, _getIeDocumentElementOffset, _getMarginBox, _getMarginExtents, _getMarginSize, _getPadBorderExtents, _getPadExtents, _getText, _hasResource, _hitchArgs, _ioAddQueryToUrl, _ioCancelAll, _ioNotifyStart, _ioSetArgs, _ioWatch, _isBodyLtr, _isDocumentOk, _keypress, _mixin, _name, _scopeName, _toArray, _toDom, _xhrObj, addClass, addOnLoad, addOnUnload, addOnWindowUnload, anim, animateProperty, attr, baseUrl, blendColors, body, byId, cache, clearCache, clone, colorFromArray, colorFromHex, colorFromRgb, colorFromString, config, connect, connectPublisher, contentBox, contentHandlers, cookie, coords, create, data, date, declare, delegate, deprecated, destroy, dijit, disconnect, dnd, doc, docScroll, dojox, empty, eval, every, exists, exit, experimental, extend, fadeIn, fadeOut, fieldToObject, filter, fixEvent, fixIeBiDiScrollLeft, forEach, formToJson, formToObject, formToQuery, fromJson, fx, getAttr, getBorderExtents, getComputedStyle, getContentBox, getIeDocumentElementOffset, getL10nName, getMarginBox, getMarginExtents, getMarginSize, getNodeProp, getObject, getPadBorderExtents, getPadExtents, getProp, getStyle, global, hasAttr, hasClass, hitch, html, i18n, indexOf, isAir, isAlien, isAndroid, isArray, isArrayLike, isAsync, isBodyLtr, isBrowser, isChrome, isCopyKey, isDescendant, isFF, isFunction, isIE, isIos, isKhtml, isMac, isMoz, isMozilla, isObject, isOpera, isQuirks, isSafari, isString, isWebKit, isWii, keys, lastIndexOf, locale, map, marginBox, mixin, moduleUrl, mouseButtons, number, objectToQuery, parser, partial, place, position, prop, publish, query, queryToObject, rawXhrPost, rawXhrPut, ready, regexp, removeAttr, removeClass, replace, replaceClass, safeMixin, scopeMap, setAttr, setContentSize, setContext, setMarginBox, setObject, setProp, setSelectable, setStyle, some, stopEvent, store, string, style, subscribe, toDom, toJson, toJsonIndentStr, toPixelValue, toggleClass, touch, trim, unsubscribe, version, when, window, withDoc, withGlobal, xhr, xhrDelete, xhrGet, xhrPost, xhrPut"
) {
    console.error("Debug export mismatch", _debug_keys.join(", "));
}
/* */

export { Animation, Color, Deferred, NodeList, Stateful, _Animation, _Line, _Url, __toPixelValue, _blockAsync, _contentHandlers, _defaultEasing, _destroyElement, _docScroll, _escapeString, _extraNames, _fade, _filterQueryResult, _fixIeBiDiScrollLeft, _getBorderExtents, _getContentBox, _getIeDocumentElementOffset, _getMarginBox, _getMarginExtents, _getMarginSize, _getPadBorderExtents, _getPadExtents, _getText, _hasResource, _hitchArgs, _ioAddQueryToUrl, _ioCancelAll, _ioNotifyStart, _ioSetArgs, _ioWatch, _isBodyLtr, _isDocumentOk, _keypress, _mixin, _name, _scopeName, _toArray, _toDom, _xhrObj, addClass, addOnLoad, addOnUnload, addOnWindowUnload, anim, animateProperty, attr, baseUrl, blendColors, body, byId, cache, clearCache, clone, colorFromArray, colorFromHex, colorFromRgb, colorFromString, config, connect, connectPublisher, contentBox, contentHandlers, cookie, coords, create, data, date, declare, delegate, deprecated, destroy, dijit, disconnect, dnd, doc, docScroll, dojox, empty, eval, every, exists, exit, experimental, extend, fadeIn, fadeOut, fieldToObject, filter, fixEvent, fixIeBiDiScrollLeft, forEach, formToJson, formToObject, formToQuery, fromJson, fx, getAttr, getBorderExtents, getComputedStyle, getContentBox, getIeDocumentElementOffset, getL10nName, getMarginBox, getMarginExtents, getMarginSize, getNodeProp, getObject, getPadBorderExtents, getPadExtents, getProp, getStyle, global, hasAttr, hasClass, hitch, html, i18n, indexOf, isAir, isAlien, isAndroid, isArray, isArrayLike, isAsync, isBodyLtr, isBrowser, isChrome, isCopyKey, isDescendant, isFF, isFunction, isIE, isIos, isKhtml, isMac, isMoz, isMozilla, isObject, isOpera, isQuirks, isSafari, isString, isWebKit, isWii, keys, lastIndexOf, locale, map, marginBox, mixin, moduleUrl, mouseButtons, number, objectToQuery, parser, partial, place, position, prop, publish, query, queryToObject, rawXhrPost, rawXhrPut, ready, regexp, removeAttr, removeClass, replace, replaceClass, safeMixin, scopeMap, setAttr, setContentSize, setContext, setMarginBox, setObject, setProp, setSelectable, setStyle, some, stopEvent, store, string, style, subscribe, toDom, toJson, toJsonIndentStr, toPixelValue, toggleClass, touch, trim, unsubscribe, version, when, window, withDoc, withGlobal, xhr, xhrDelete, xhrGet, xhrPost, xhrPut } from "dojo/main";
export * as arrayUtil from "dojo/_base/array";
export * as lang from "dojo/_base/lang";

export * as has from "dojo/has";
export * as dom from "dojo/dom";
export * as on from "dojo/on";
export * as mouse from "dojo/mouse";
export * as domClass from "dojo/dom-class";
export * as domForm from "dojo/dom-form";
export * as topic from "dojo/topic";
export * as DeferredFull from "dojo/Deferred";
export * as domConstruct from "dojo/dom-construct";
export * as all from "dojo/promise/all";
export * as aspect from "dojo/aspect";
export * as domStyle from "dojo/dom-style";
export * as Evented from "dojo/Evented";
export * as json from "dojo/json";
export * as request from "dojo/request";
export * as script from "dojo/request/script";
export * as iframe from "dojo/request/iframe";

import "dojo/Stateful";
import "dojo/cookie";
import "dojo/html";

export * as Observable from "dojo/store/Observable";
export * as QueryResults from "dojo/store/util/QueryResults";
export * as SimpleQueryEngine from "dojo/store/util/SimpleQueryEngine";

import "dojo/i18n";
// @ts-expect-error
import * as nlsHPCC from "dojo/i18n!./nls/hpcc";
import nlsHPCCT from "./nls/hpcc";

export default nlsHPCC as typeof nlsHPCCT.root;
