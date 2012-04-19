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
	"dojo/_base/declare"
], function(declare) {
	return declare(null, {
		domId : "",
		editor : null,
		markers : [],

		// The constructor    
		constructor : function(args) {
			declare.safeMixin(this, args);
			var _editor = this.editor = CodeMirror.fromTextArea(document.getElementById(this.domId), {
				tabMode : "indent",
				matchBrackets : true,
				gutter : true,
				lineNumbers : true
			});
		},

		clearErrors : function(errWarnings) {
			for ( var i = 0; i < this.markers.length; ++i) {
				this.editor.clearMarker(this.markers[i]);
			}
			this.markers = [];
		},

		setErrors : function(errWarnings) {
			for ( var i = 0; i < errWarnings.length; ++i) {
				this.markers.push(this.editor.setMarker(parseInt(
						errWarnings[i].LineNo, 10) - 1, "",
						errWarnings[i].Severity + "Line"));
			}
		},

		setCursor : function(line, col) {
			this.editor.setCursor(line - 1, col - 1);
			this.editor.focus();
		},

		setText : function(ecl) {
			this.editor.setValue(ecl);
		},

		getText : function() {
			return this.editor.getValue();
		}
	});
});