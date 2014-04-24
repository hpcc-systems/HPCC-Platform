/*##############################################################################
#   HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/dom-construct",

    "dijit/registry",

    "hpcc/_Widget",

    "dojo/text!../templates/IFrameWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/form/Button",
    "dijit/layout/ContentPane"
], function (declare, lang, i18n, nlsHPCC, domConstruct,
                registry,
                _Widget,
                template) {
    return declare("IFrameWidget", [_Widget], {
        templateString: template,
        baseClass: "IFrameWidget",
        i18n: nlsHPCC,

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
            this.contentPane = registry.byId(this.id + "ContentPane");
        },

        resize: function (args) {
            this.inherited(arguments);
            if (this.borderContainer) {
                this.borderContainer.resize();
            }
        },

        _onNewPageNoFrame: function (event) {
            var win = window.open(this.params.src);
            win.focus();
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.contentPane.set("content", domConstruct.create("iframe", {
                id: this.id + "IFrame",
                src: this.params.src,
                style: "border:1px solid lightgray; width: 100%; height: 100%"
            }));
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            this._onRefresh();
        }
    });
});
