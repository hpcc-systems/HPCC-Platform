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
	"dojo/_base/fx",
	"dojo/_base/window",
	"dojo/dom",
	"dojo/dom-style",
	"dojo/dom-geometry",
	"dojo/io-query",
	"dojo/ready",

	"dijit/registry"
], function (fx, baseWindow, dom, domStyle, domGeometry, ioQuery, ready, registry) {
    var initUi = function () {
        var wuid = "W20120722-100052";//"W20120601-121930";//"W20120530-153214";//
        var graphName = "";

        var urlWuid = ioQuery.queryToObject(dojo.doc.location.search.substr((dojo.doc.location.search.substr(0, 1) == "?" ? 1 : 0)))["Wuid"];
        if (urlWuid) {
            wuid = urlWuid;
        }
        var urlGraphName = ioQuery.queryToObject(dojo.doc.location.search.substr((dojo.doc.location.search.substr(0, 1) == "?" ? 1 : 0)))["GraphName"];
        if (urlGraphName) {
            graphName = urlGraphName;
        }

        var graphControl = registry.byId("appLayout");
        graphControl.setWuid(wuid, graphName);
    },

	startLoading = function (targetNode) {
	    var overlayNode = dom.byId("loadingOverlay");
	    if ("none" == domStyle.get(overlayNode, "display")) {
	        var coords = domGeometry.getMarginBox(targetNode || baseWindow.body());
	        domGeometry.setMarginBox(overlayNode, coords);
	        domStyle.set(dom.byId("loadingOverlay"), {
	            display: "block",
	            opacity: 1
	        });
	    }
	},

	endLoading = function () {
	    fx.fadeOut({
	        node: dom.byId("loadingOverlay"),
	        duration: 175,
	        onEnd: function (node) {
	            domStyle.set(node, "display", "none");
	        }
	    }).play();
	}

    return {
        init: function () {
            startLoading();
            ready(function () {
                initUi();
                endLoading();
            });
        }
    };
});