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
    "dojo/_base/array",
    "dojo/_base/Deferred",
    "dojo/_base/lang",
    "dojo/data/ObjectStore",
    "dojo/store/util/QueryResults",
    "dojo/store/Observable",
    "dojo/dom-construct",

    "dojox/xml/parser",
    "dojox/xml/DomParser",
    "dojox/html/entities",

    "hpcc/ESPBase",
    "hpcc/ESPRequest",
    "hpcc/WsWorkunits"
], function (declare, arrayUtil, Deferred, lang, ObjectStore, QueryResults, Observable, domConstruct,
            parser, DomParser, entities,
            ESPBase, ESPRequest, WsWorkunits) {

    var Store = declare([ESPRequest.Store, ESPBase], {
        service: "WsWorkunits",
        action: "WUResult",
        responseQualifier: "Result",
        responseTotalQualifier: "Total",
        idProperty: "rowNum",
        startProperty: "Start",
        countProperty: "Count",
        preRequest: function (request) {
            if (this.name) {
                request['LogicalName'] = this.name;
            } else {
                request['Wuid'] = this.wuid;
                request['Sequence'] = this.sequence;
            }
        },
        preProcessResponse: function (response, request) {
            var xml = "<Result>" + response.Result + "</Result>";
            var domXml = parser.parse(xml);
            rows = this.getValues(domXml, "Row");
            arrayUtil.forEach(rows, function (item, index) {
                item.rowNum = request.Start + index + 1;
            });
            response.Result = rows;
        }
    });

    var Result = declare(null, {
        store: null,
        Total: "-1",

        constructor: function (args) {
            declare.safeMixin(this, args);
            if (this.Sequence != null) {
                this.store = new Store({
                    wuid: this.Wuid,
                    sequence: this.Sequence,
                    isComplete: this.isComplete()
                });
            } else {
                this.store = new Store({
                    wuid: this.Wuid,
                    cluster: this.Cluster,
                    name: this.Name,
                    isComplete: true
                });
            }
        },

        getName: function () {
            return this.Name;
        },

        getID: function () {
            if (this.Sequence != null) {
                return this.Sequence;
            }
            return this.Name;
        },

        isComplete: function () {
            return this.Total != "-1";
        },

        canShowResults: function () {
            if (lang.exists("Sequence", this)) { //  Regular WU result
                return true;
            } else if (lang.exists("RecordCount", this) && this.RecordCount != "") { //  DFU Sprayed CSV File will fail here
                return true;
            }
            return false;
        },

        getFirstSchemaNode: function (node, name) {
            if (node && node.attributes) {
                if ((node.baseName && node.baseName == name) || (node.localName && node.localName == name) || (typeof (node.getAttribute) != "undefined" && node.getAttribute("name") == name)) {
                    return node;
                }
            }
            for (var i = 0; i < node.childNodes.length; ++i) {
                var retVal = this.getFirstSchemaNode(node.childNodes[i], name);
                if (retVal) {
                    return retVal;
                }
            }
            return null;
        },

        getFirstSequenceNode: function (schemaNode) {
            var row = this.getFirstSchemaNode(schemaNode, "Row");
            if (!row)
                return null;
            var complexType = this.getFirstSchemaNode(row, "complexType");
            if (!complexType)
                return null;
            return this.getFirstSchemaNode(complexType, "sequence");
        },

        rowToTable: function (cell) {
            var table = domConstruct.create("table", { border: 1, cellspacing: 0, width: "100%" });
            if (cell && cell.Row) {
                if (!cell.Row.length) {
                    cell.Row = [cell.Row];
                }

                for (var i = 0; i < cell.Row.length; ++i) {
                    if (i == 0) {
                        var tr = domConstruct.create("tr", null, table);
                        for (key in cell.Row[i]) {
                            var th = domConstruct.create("th", { innerHTML: entities.encode(key) }, tr);
                        }
                    }
                    var tr = domConstruct.create("tr", null, table);
                    for (var key in cell.Row[i]) {
                        if (cell.Row[i][key]) {
                            if (cell.Row[i][key].Row) {
                                var td = domConstruct.create("td", null, tr);
                                td.appendChild(this.rowToTable(cell.Row[i][key]));
                            } else {
                                var td = domConstruct.create("td", { innerHTML: entities.encode(cell.Row[i][key]) }, tr);
                            }
                        } else {
                            var td = domConstruct.create("td", { innerHTML: "" }, tr);
                        }
                    }
                }
            }
            return table;
        },

        getRowStructureFromSchema: function (parentNode) {
            var retVal = [];
            var sequence = this.getFirstSequenceNode(parentNode, "sequence");
            if (!sequence)
                return retVal;

            for (var i = 0; i < sequence.childNodes.length; ++i) {
                var node = sequence.childNodes[i];
                if (typeof (node.getAttribute) != "undefined") {
                    var name = node.getAttribute("name");
                    var type = node.getAttribute("type");
                    if (name && type) {
                        retVal.push({
                            label: name,
                            field: name,
                            width: this.extractWidth(type, name) * 9,
                            className: "resultGridCell",
                            sortable: false
                        });
                    }
                    if (node.hasChildNodes()) {
                        var context = this;
                        retVal.push({
                            label: name,
                            field: name,
                            formatter: function (cell, row, grid) {
                                var div = document.createElement("div");
                                div.appendChild(context.rowToTable(cell));
                                return div.innerHTML;
                            },
                            width: this.getRowWidth(node) * 9,
                            className: "resultGridCell",
                            sortable: false
                        });
                    }
                }
            }
            return retVal;
        },

        getRowStructureFromData: function (rows) {
            var retVal = [];
            for (var key in rows[0]) {
                if (key != "myInjectedRowNum") {
                    var context = this;
                    retVal.push({
                        label: key,
                        field: key,
                        formatter: function (cell, row, grid) {
                            if (cell && cell.Row) {
                                var div = document.createElement("div");
                                div.appendChild(context.rowToTable(cell));
                                return div.innerHTML;
                            }
                            return cell;
                        },
                        width: context.extractWidth("string12", key) * 9,
                        className: "resultGridCell"
                    });
                }
            }
            return retVal;
        },

        getStructure: function () {
            var structure = [
                {
                    cells: [
                        [
                            {
                                label: "##", field: this.store.idProperty, width: 54, className: "resultGridCell", sortable: false
                            }
                        ]
                    ]
                }
            ];

            var dom = parser.parse(this.XmlSchema);
            var dataset = this.getFirstSchemaNode(dom, "Dataset");
            var innerStruct = this.getRowStructureFromSchema(dataset);
            for (var i = 0; i < innerStruct.length; ++i) {
                structure[0].cells[structure[0].cells.length - 1].push(innerStruct[i]);
            }
            return structure[0].cells[0];
        },

        fetchStructure: function (callback) {
            if (this.XmlSchema) {
                callback(this.getStructure());
            } else {
                var context = this;

                var request = {};
                if (this.Name) {
                    request['LogicalName'] = this.Name;
                } else {
                    request['Wuid'] = this.Wuid;
                    request['Sequence'] = this.Sequence;
                }
                request['Start'] = 0;
                request['Count'] = 1;
                WsWorkunits.WUResult({
                    request: request,
                    load: function (response) {
                        if (lang.exists("WUResultResponse.Result", response)) {
                            context.XmlSchema = "<Result>" + response.WUResultResponse.Result + "</Result>";
                            callback(context.getStructure());
                        }
                        /*
                        if (rows.length) {
                            var innerStruct = context.getRowStructureFromData(rows);
                            for (var i = 0; i < innerStruct.length; ++i) {
                                structure[0].cells[structure[0].cells.length - 1].push(innerStruct[i]);
                            }
                        }
                        */
                    }
                });
            }
        },

        getRowWidth: function (parentNode) {
            var retVal = 0;
            var sequence = this.getFirstSequenceNode(parentNode, "sequence");
            if (!sequence)
                return retVal;

            for (var i = 0; i < sequence.childNodes.length; ++i) {
                var node = sequence.childNodes[i];
                if (typeof (node.getAttribute) != "undefined") {
                    var name = node.getAttribute("name");
                    var type = node.getAttribute("type");
                    if (name && type) {
                        retVal += this.extractWidth(type, name);
                    } else if (node.hasChildNodes()) {
                        retVal += this.getRowWidth(node);
                    }
                }
            }
            return retVal;
        },

        extractWidth: function (type, name) {
            var retVal = -1;

            switch (type) {
                case "xs:boolean":
                    retVal = 5;
                    break;
                case "xs:integer":
                    retVal = 8;
                    break;
                case "xs:nonNegativeInteger":
                    retVal = 8;
                    break;
                case "xs:double":
                    retVal = 8;
                    break;
                case "xs:string":
                    retVal = 32;
                    break;
                default:
                    var numStr = "0123456789";
                    var underbarPos = type.lastIndexOf("_");
                    var length = underbarPos > 0 ? underbarPos : type.length;
                    var i = length - 1;
                    for (; i >= 0; --i) {
                        if (numStr.indexOf(type.charAt(i)) == -1)
                            break;
                    }
                    if (i + 1 < length) {
                        retVal = parseInt(type.substring(i + 1, length));
                    }
                    if (type.indexOf("data") == 0) {
                        retVal *= 2;
                    }
                    break;
            }
            if (retVal < name.length)
                retVal = name.length;

            return retVal;
        },

        getStore: function () {
            return this.store;
        },

        getObjectStore: function () {
            return new ObjectStore({
                objectStore: this.store
            });
        },

        getLoadingMessage: function () {
            if (lang.exists("wu.state", this)) {
                return "<span class=\'dojoxGridWating\'>[" + this.wu.state + "]</span>";
            }
            return "<span class=\'dojoxGridWating\'>[unknown]</span>";
        },

        getECLRecord: function () {
            var retVal = "RECORD\n";
            for (var i = 0; i < this.ECLSchemas.ECLSchemaItem.length; ++i) {
                retVal += "\t" + this.ECLSchemas.ECLSchemaItem[i].ColumnType + "\t" + this.ECLSchemas.ECLSchemaItem[i].ColumnName + ";\n";
            }
            retVal += "END;\n";
            return retVal;
        }
    });

    return {
        CreateWUResultObjectStore: function (options) {
            var store = new Store(options);
            store = Observable(store);
            var objStore = new ObjectStore({ objectStore: store });
            return objStore;
        },

        Get: function (params) {
            return new Result(params);
        }
    }
});
