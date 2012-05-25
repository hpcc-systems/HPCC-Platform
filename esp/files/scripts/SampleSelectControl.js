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
	"dojo/_base/xhr",
	"dojo/data/ItemFileReadStore",
	"dijit/form/Select"
], function (declare, xhr, ItemFileReadStore, Select) {
	return declare(null, {
		id: null,
		samplesURL: null,

		onNewSelection: function (eclText) {
		},

		constructor: function (args) {
			declare.safeMixin(this, args);
			var sampleStore = new dojo.data.ItemFileReadStore({
				url: this.samplesURL
			});

			var context = this;
			var select = new dijit.form.Select({
				name: this.id,
				store: sampleStore,
				value: "default.ecl",
				maxHeight: -1,
				style: {
					padding: 0
				},
				onChange: function () {
					var filename = dijit.byId(this.id).get("value");
					xhr.get({
						url: "ecl/" + filename,
						handleAs: "text",
						load: function (eclText) {
							context.onNewSelection(eclText);
						},
						error: function () {
						}
					});
				}
			}, this.id);
			try {
				select.startup();
			} catch (e) {
			}
		}

	});
});