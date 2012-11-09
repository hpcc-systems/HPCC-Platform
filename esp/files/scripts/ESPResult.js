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
    "dojo/_base/Deferred",
    "dojo/data/ObjectStore",
    "dojo/dom-construct",

    "dojox/xml/parser",
    "dojox/xml/DomParser",
    "dojox/html/entities",

    "hpcc/WsWorkunits",
    "hpcc/ESPBase"
], function (declare, Deferred, ObjectStore, domConstruct,
            parser, DomParser, entities,
            WsWorkunits, ESPBase) {
    return declare(ESPBase, {
        store: null,
        Total: "-1",

        constructor: function (args) {
            declare.safeMixin(this, args);
            if (this.Sequence != null) {
                this.store = new WsWorkunits.WUResult({
                    wuid: this.wuid,
                    sequence: this.Sequence,
                    isComplete: this.isComplete()
                });
            } else {
                this.store = new WsWorkunits.WUResult({
                    wuid: this.wuid,
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

                for (i = 0; i < cell.Row.length; ++i) {
                    if (i == 0) {
                        var tr = domConstruct.create("tr", null, table);
                        for (key in cell.Row[i]) {
                            var th = domConstruct.create("th", { innerHTML: entities.encode(key) }, tr);
                        }
                    }
                    var tr = domConstruct.create("tr", null, table);
                    for (key in cell.Row[i]) {
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
                            name: name,
                            field: name,
                            width: this.extractWidth(type, name),
                            classes: "resultGridCell"
                        });
                    }
                    if (node.hasChildNodes()) {
                        var context = this;
                        retVal.push({
                            name: name,
                            field: name,
                            formatter: function (cell, row, grid) {
                                var div = document.createElement("div");
                                div.appendChild(context.rowToTable(cell));
                                return div.innerHTML;
                            },
                            width: this.getRowWidth(node),
                            classes: "resultGridCell"
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
                        name: key,
                        field: key,
                        formatter: function (cell, row, grid) {
                            if (cell && cell.Row) {
                                var div = document.createElement("div");
                                div.appendChild(context.rowToTable(cell));
                                return div.innerHTML;
                            }
                            return cell;
                        },
                        width: context.extractWidth("string12", key),
                        classes: "resultGridCell"
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
                                name: "##", field: this.store.idProperty, width: "40px", classes: "resultGridCell"
                            }
                        ]
                    ]
                }
            ];

            if (this.XmlSchema) {
                var dom = parser.parse(this.XmlSchema);
                var dataset = this.getFirstSchemaNode(dom, "Dataset");
                var innerStruct = this.getRowStructureFromSchema(dataset);
                for (var i = 0; i < innerStruct.length; ++i) {
                    structure[0].cells[structure[0].cells.length - 1].push(innerStruct[i]);
                }
            } else {
                var context = this;
                Deferred.when(this.store.query("*", {
                    start: 0,
                    count: 1,
                    sync: true
                }), function (rows) {
                    if (rows.length) {
                        var innerStruct = context.getRowStructureFromData(rows);
                        for (var i = 0; i < innerStruct.length; ++i) {
                            structure[0].cells[structure[0].cells.length - 1].push(innerStruct[i]);
                        }
                    }
                });
            }
            return structure;
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

        getObjectStore: function () {
            return ObjectStore({
                objectStore: this.store
            });
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
});
