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
    "dojo/dom-construct",

    "dojox/xml/parser",
    "dojox/html/entities",

    "hpcc/ESPBase",
    "hpcc/ESPRequest",
    "hpcc/WsWorkunits"
], function (declare, arrayUtil, Deferred, lang, domConstruct,
            parser, entities,
            ESPBase, ESPRequest, WsWorkunits) {

    var Store = declare([ESPRequest.Store, ESPBase], {
        service: "WsWorkunits",
        action: "WUResult",
        responseQualifier: "WUResultResponse.Result",
        responseTotalQualifier: "WUResultResponse.Total",
        idProperty: "__hpcc_id",
        startProperty: "Start",
        countProperty: "Count",
        useSingletons: false,
        preRequest: function (request) {
            if (this.name && this.cluster) {
                this.idPrefix = this.name + "_" + this.cluster;
                request['LogicalName'] = this.name;
                request['Cluster'] = this.cluster;
            } else if (this.name) {
                this.idPrefix = this.name;
                request['LogicalName'] = this.name;
            } else {
                this.idPrefix = this.wuid + "_" + this.sequence;
                request['Wuid'] = this.wuid;
                request['Sequence'] = this.sequence;
            }
            if (request.includeXmlSchema) {
                request['SuppressXmlSchema'] = false;
            } else {
                request['SuppressXmlSchema'] = true;
            }
        },
        preProcessResponse: function (response, request) {
            if (lang.exists("Result.Row", response)) {
                var context = this;
                arrayUtil.forEach(response.Result.Row, function (item, index) {
                    item.__hpcc_rowNum = request.Start + index + 1;
                    item.__hpcc_id = context.idPrefix + "_" + item.__hpcc_rowNum;
                });
                response.Result = response.Result.Row;
            }
        }
    });

    var Result = declare(null, {
        store: null,
        Total: "-1",

        constructor: function (args) {
            if (args) {
                declare.safeMixin(this, args);
            }
            if (lang.exists("Sequence", this)) {
                this.store = new Store({
                    wuid: this.Wuid,
                    sequence: this.Sequence,
                    isComplete: this.isComplete()
                });
            } else if (lang.exists("Name", this) && lang.exists("ClusterName", this)) {
                this.store = new Store({
                    wuid: this.Wuid,
                    cluster: this.ClusterName,
                    name: this.Name,
                    isComplete: true
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
                row = schemaNode;
            var complexType = this.getFirstSchemaNode(row, "complexType");
            if (!complexType)
                return null;
            return this.getFirstSchemaNode(complexType, "sequence");
        },

        rowToTable: function (cell, __row, node) {
            var table = domConstruct.create("table", { border: 1, cellspacing: 0, width: "100%" }, node);
            if (lang.exists("Row", cell)) {
                cell = cell.Row;
            } 
            if (Object.prototype.toString.call(cell) === '[object Array]') {
                for (var i = 0; i < cell.length; ++i) {
                    if (i == 0) {
                        var tr = domConstruct.create("tr", null, table);
                        for (var key in cell[i]) {
                            var th = domConstruct.create("th", { innerHTML: entities.encode(key) }, tr);
                        }
                    }
                    var tr = domConstruct.create("tr", null, table);
                    for (var key in cell[i]) {
                        if (cell[i][key]) {
                            if (Object.prototype.toString.call(cell[i][key]) === '[object Object]' || Object.prototype.toString.call(cell[i][key]) === '[object Array]') {
                                var td = domConstruct.create("td", null, tr);
                                this.rowToTable(cell[i][key], cell[i], td);
                            } else if (key.indexOf("__html", key.length - "__html".length) !== -1) {
                                var td = domConstruct.create("td", { innerHTML : cell[i][key] }, tr);
                            } else if (key.indexOf("__javascript", key.length - "__javascript".length) !== -1) {
                                var td = domConstruct.create("td", null, tr);
                                this.injectJavascript(cell[i][key], cell[i], td);
                            } else {
                                var val = cell[i][key];
                                var td = domConstruct.create("td", { innerHTML: Object.prototype.toString.call(val) === '[object String]' ? entities.encode(val) : val }, tr);
                            }
                        } else {
                            var td = domConstruct.create("td", { innerHTML: "" }, tr);
                        }
                    }
                }
            }
        },
        injectJavascript : function(__cellContent, __row, __cell, __width) {
            //  Add paragraph so cells can valign  ---
            domConstruct.create("p", {
                style: {
                    height : "1px"
                },
                innerHTML: "&nbsp;"
            }, __cell);
            try {
                eval(__cellContent);
            } catch (e) {
                __cell.innerHTML = "<b>Error:</b>&nbsp;&nbsp;" + entities.encode(e.message) + "<br>" + entities.encode(__cellContent);
            }
        },
        parseName: function (nameObj) {
            nameObj.width = 500;
            var titleParts = nameObj.name.split("__");
            if (titleParts.length >= 3) {
                var specifiedWidth = parseInt(titleParts[titleParts.length - 2]);
                if (!isNaN(specifiedWidth)) {
                    nameObj.width = specifiedWidth;
                    titleParts = titleParts.slice(0, titleParts.length - 1);
                }
            }
            titleParts = titleParts.slice(0, titleParts.length - 1);
            nameObj.displayName = titleParts.join("__");
        },
        getRowStructureFromSchema: function (parentNode, prefix) {
            var sequence = this.getFirstSequenceNode(parentNode, "sequence");
            if (!sequence)
                return null;

            var retVal = [];
            for (var i = 0; i < sequence.childNodes.length; ++i) {
                var node = sequence.childNodes[i];
                if (typeof (node.getAttribute) != "undefined") {
                    var name = node.getAttribute("name");
                    var type = node.getAttribute("type");
                    var children = this.getRowStructureFromSchema(node, name + "_");
                    var context = this;
                    if (name && name.indexOf("__hidden", name.length - "__hidden".length) !== -1) {
                    } else if (name && type) {
                        if (name.indexOf("__html", name.length - "__html".length) !== -1) {
                            var nameObj = {
                                name: name
                            };
                            this.parseName(nameObj);
                            retVal.push({
                                label: nameObj.displayName,
                                field: prefix + name,
                                width: nameObj.width,
                                className: "resultGridCell",
                                formatter: function (cell, row) {
                                    return cell;
                                },
                                sortable: false
                            });
                        } else if (name.indexOf("__javascript", name.length - "__javascript".length) !== -1) {
                            var nameObj = {
                                name: name
                            };
                            this.parseName(nameObj);
                            retVal.push({
                                label: nameObj.displayName,
                                field: prefix + name,
                                width: nameObj.width,
                                className: "resultGridCell",
                                renderCell: function(row, cell, node, options) {
                                    context.injectJavascript(cell, row, node, this.width)
                                },
                                sortable: false
                            });
                        } else {
                            retVal.push({
                                label: name,
                                field: prefix + name,
                                width: this.extractWidth(type, name) * 9,
                                className: "resultGridCell",
                                sortable: false
                            });
                        }
                    } else if (children) {
                        var childWidth = 10;  //  Allow for html table
                        arrayUtil.forEach(children, function(item, idx) {
                            childWidth += item.width;
                        });
                        /*
                        retVal.push({
                            label: name,
                            children: children,
                            width: childWidth,
                            className: "resultGridCell",
                            sortable: false
                        });
                        */
                        retVal.push({
                            label: name,
                            field: name,
                            renderCell: function(row, cell, node, options) {
                                context.rowToTable(cell, row, node);
                            },
                            width: childWidth,
                            className: "resultGridCell",
                            sortable: false
                        });
                    }
                }
            }
            return retVal.length ? retVal : null;
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
                            if (Object.prototype.toString.call(cell) === '[object Object]' || Object.prototype.toString.call(cell) === '[object Array]') {
                                var div = document.createElement("div");
                                context.rowToTable(cell, row, div);
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
                                label: "##", field: "__hpcc_rowNum", width: 54, className: "resultGridCell", sortable: false
                            }
                        ]
                    ]
                }
            ];

            var dom = parser.parse(this.XmlSchema);
            var dataset = this.getFirstSchemaNode(dom, "Dataset");
            var innerStruct = this.getRowStructureFromSchema(dataset, "");
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
                if (this.Wuid && lang.exists("Sequence", this)) {
                    request['Wuid'] = this.Wuid;
                    request['Sequence'] = this.Sequence;
                } else if (this.Name && this.ClusterName) {
                    request['LogicalName'] = this.Name;
                    request['Cluster'] = this.ClusterName;
                } else if (this.Name) {
                    request['LogicalName'] = this.Name;
                }
                request['Start'] = 0;
                request['Count'] = 1;
                WsWorkunits.WUResult({
                    request: request,
                    load: function (response) {
                        if (lang.exists("WUResultResponse.Result.XmlSchema.xml", response)) {
                            context.XmlSchema = "<Result>" + response.WUResultResponse.Result.XmlSchema.xml + "</Result>";
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

        fetchContent: function () {
            var deferred = new Deferred()
            var context = this;
            this.store.query({
                Start: 0,
                Count: 1
            }).total.then(function(total) {
                context.store.query({
                    Start: 0,
                    Count: total
                }).then(function(results) {
                    deferred.resolve(results);
                });
            });
            return deferred.promise;
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
        Get: function (params) {
            return new Result(params);
        }
    }
});
