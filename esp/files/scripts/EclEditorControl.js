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
	"dojo/_base/declare"
], function (declare) {
	return declare(null, {
		domId: "",
		editor: null,
		markers: [],
		highlightLines: [],

		// The constructor    
		constructor: function (args) {
			declare.safeMixin(this, args);
			this.editor = CodeMirror.fromTextArea(document.getElementById(this.domId), {
				tabMode: "indent",
				matchBrackets: true,
				gutter: true,
				lineNumbers: true
			});
		},

		clearErrors: function (errWarnings) {
			for (var i = 0; i < this.markers.length; ++i) {
				this.editor.clearMarker(this.markers[i]);
			}
			this.markers = [];
		},

		setErrors: function (errWarnings) {
			for (var i = 0; i < errWarnings.length; ++i) {
				this.markers.push(this.editor.setMarker(parseInt(
						errWarnings[i].LineNo, 10) - 1, "",
						errWarnings[i].Severity + "Line"));
			}
		},

		setCursor: function (line, col) {
			this.editor.setCursor(line - 1, col - 1);
			this.editor.focus();
		},

		clearHighlightLines: function () {
			for (var i = 0; i < this.highlightLines.length; ++i) {
				this.editor.setLineClass(this.highlightLines[i], null, null);
			}
		},

		highlightLine: function (line) {
			this.highlightLines.push(this.editor.setLineClass(line - 1, "highlightline"));
		},

		setText: function (ecl) {
			this.editor.setValue(ecl);
		},

		getText: function () {
			return this.editor.getValue();
		}
	});
});