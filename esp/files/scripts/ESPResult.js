﻿/*##############################################################################
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
	"hpcc/WsWorkunits",
	"hpcc/ESPBase"
], function (declare, Deferred, ObjectStore, WsWorkunits, ESPBase) {
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

		getStructure: function () {
			var retVal = [];
			retVal.push({
				name: "##",
				field: this.store.idProperty,
				width: "40px"
			});
			if (this.ECLSchemas) {
				for (var i = 0; i < this.ECLSchemas.ECLSchemaItem.length; ++i) {
					retVal.push({
						name: this.ECLSchemas.ECLSchemaItem[i].ColumnName,
						field: this.ECLSchemas.ECLSchemaItem[i].ColumnName,
						width: this.extractWidth(this.ECLSchemas.ECLSchemaItem[i].ColumnType, this.ECLSchemas.ECLSchemaItem[i].ColumnName)
					});
				}
			} else {
				var context = this;
				Deferred.when(this.store.query("*", {
					start: 0,
					count: 1,
					sync: true
				}), function (rows) {
					if (rows.length) {
						for (var key in rows[0]) {
							if (key != "myInjectedRowNum") {
								retVal.push({
									name: key,
									field: key,
									width: context.extractWidth("string12", key)
								});
							}
						}
					}
				});
			}
			return retVal;
		},

		extractWidth: function (type, name) {
			var numStr = "0123456789";
			var retVal = -1;
			var i = type.length;
			while (i >= 0) {
				if (numStr.indexOf(type.charAt(--i)) == -1)
					break;
			}
			if (i > 0)
				retVal = parseInt(type.substring(i + 1, type.length));

			if (retVal < name.length)
				retVal = name.length;

			return Math.round(retVal * 2 / 3);
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
