// Vite shim for the "src-dojo/index" module specifier.  Importing the compiled
// legacy entry keeps modern React components and the legacy script entry on one
// Dojo runtime instance.
//
// NOTE: This list must include every named export of src-dojo/index.ts that is
// actually imported anywhere under src/**/*.ts or src-react/**/*.tsx. If a build
// or type error reports a missing export here, add it below (see src-dojo/dojo.ts,
// dijit.ts, dojox.ts, dgrid.ts for the original source of each symbol).

import g from "../lib/src-dojo/index.js";

export default g.default;

export const registry = g.registry;
export const arrayUtil = g.arrayUtil;
export const declare = g.declare;
export const lang = g.lang;
export const aspect = g.aspect;
export const domClass = g.domClass;
export const domForm = g.domForm;
export const domStyle = g.domStyle;
export const Evented = g.Evented;
export const json = g.json;
export const on = g.on;
export const query = g.query;
export const Stateful = g.Stateful;
export const Tooltip = g.Tooltip;
export const ColumnResizer = g.ColumnResizer;
export const CompoundColumns = g.CompoundColumns;
export const DijitRegistry = g.DijitRegistry;
export const Grid = g.Grid;
export const Keyboard = g.Keyboard;
export const OnDemandGrid = g.OnDemandGrid;
export const Selection = g.Selection;
export const Observable = g.Observable;
export const Deferred = g.Deferred;
export const DeferredFull = g.DeferredFull;
export const domConstruct = g.domConstruct;
export const dojoxHtmlEntities = g.dojoxHtmlEntities;
export const dojoxXmlParser = g.dojoxXmlParser;
export const config = g.config;
export const cookie = g.cookie;
export const QueryResults = g.QueryResults;
export const SimpleQueryEngine = g.SimpleQueryEngine;
export const topic = g.topic;
export const has = g.has;
export const all = g.all;
export const xhr = g.xhr;
export const request = g.request;
export const script = g.script;
export const iframe = g.iframe;
export const StoreMixin = g.StoreMixin;
export const Pagination = g.Pagination;
export const mouse = g.mouse;
export const selector = g.selector;
export const tree = g.tree;
export const editor = g.editor;
export const dom = g.dom;
export const put = g.put;
