/*##############################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */
define([
	"dojo/_base/declare",
	"dojo/data/ObjectStore",
	"hpcc/WsWorkunits",
	"hpcc/ESPBase"
], function (declare, ObjectStore, WsWorkunits, ESPBase) {
	return declare(ESPBase, {
		store: null,
		Total: "-1",

		constructor: function (args) {
			declare.safeMixin(this, args);
			this.store = new WsWorkunits.WUResult({
				wuid: this.wuid,
				sequence: this.Sequence,
				isComplete: this.isComplete()
			});
		},

		getName: function () {
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
			for (var i = 0; i < this.ECLSchemas.length; ++i) {
				retVal.push({
					name: this.ECLSchemas[i].ColumnName,
					field: this.ECLSchemas[i].ColumnName,
					width: this.extractWidth(this.ECLSchemas[i].ColumnType, this.ECLSchemas[i].ColumnName)
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
			for (var i = 0; i < this.ECLSchemas.length; ++i) {
				retVal += "\t" + this.ECLSchemas[i].ColumnType + "\t" + this.ECLSchemas[i].ColumnName + ";\n";
			}
			retVal += "END;\n";
			return retVal;
		}
	});
});
