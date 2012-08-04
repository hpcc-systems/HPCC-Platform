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