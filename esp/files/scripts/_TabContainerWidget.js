define([
    "dojo/_base/declare", // declare
    "dojo/_base/lang", // lang.mixin
    "dojo/dom",
    "dojo/hash",
    "dojo/router",

    "dijit/registry",
    "dijit/layout/_LayoutWidget"
], function (declare, lang, dom, hash, router,
    registry, _LayoutWidget) {

    return declare("_TabContainerWidget", [_LayoutWidget], {
        //  Assumptions:
        //    this.id + "BorderContainer" may exist.
        //    this.id + "TabContainer" exits.
        //    Child Tab Widgets ID have an underbar after the parent ID -> "${ID}_thisid" (this id for automatic back/forward button support.

        baseClass: "_TabContainerWidget",
        borderContainer: null,
        _tabContainer: null,

        disableHashing: 0,

        //  String helpers  ---
        idToPath: function (id) {
            var obj = id.split("_");
            return "/" + obj.join("/");
        },

        pathToId: function (path) {
            var obj = path.split("/");
            obj.splice(0, 1);
            return obj.join("_");
        },

        startsWith: function (tst, str) {
            if (tst.length > str.length) {
                return false;
            }
            for (var i = 0; i < tst.length; ++i) {
                if (tst.charAt(i) !== str.charAt(i)) {
                    return false;
                }
            }
            return true;
        },

        buildRendering: function () {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
            this._tabContainer = registry.byId(this.id + "TabContainer");

            var context = this;
            this._tabContainer.watch("selectedChildWidget", function (name, oval, nval) {
                context.onNewTabSelection({
                    oldWidget: oval,
                    newWidget: nval
                });
            });
        },

        startup: function () {
            if (this._started) {
                return;
            }
            this.inherited(arguments);

            var context = this;
            var obj = router.register(this.getPath() + "/:sel", function (evt) {
                context.routerCallback(evt, true);
            });
            router.registerBefore(this.getPath() + "/:sel/*other", function (evt) {
                context.routerCallback(evt, false);
            });
            router.startup();
        },

        resize: function (args) {
            this.inherited(arguments);
            if (this.borderContainer) {
                this.borderContainer.resize();
            } else {
                this._tabContainer.resize();
            }
        },

        layout: function (args) {
            this.inherited(arguments);
        },

        destroy: function (args) {
            this.inherited(arguments);
        },

        //  Hash Helpers  ---
        onNewTabSelection: function (notification) {
            var currHash = hash();
            var newHash = this.getSelectedPath();
            if (newHash != this.idToPath(notification.newWidget.id)) {
                var d = 0;
            }
            if (this.disableHashing) {
                this.go(this.getSelectedPath(), false, true);
            } else {
                var overwrite = this.startsWith(currHash, newHash);
                this.go(this.getSelectedPath(), overwrite);
            }
        },

        go: function (path, replace, noHash) {
            console.log(this.id + ".go(" + path + ", " + replace + ", " + noHash + ")");
            if (noHash) {
                var d = 0;
            } else {
                hash(path, replace);
            }
            router._handlePathChange(path);
        },

        routerCallback: function (evt) {
            var currSel = this.getSelectedChild();
            var newSel = this.id + "_" + evt.params.sel;
            if (!currSel || currSel.id != newSel) {
                this.selectChild(newSel, null);
            }
            if (this.initTab) {
                this.initTab();
            }
        },

        getPath: function () {
            return this.idToPath(this.id);
        },

        getSelectedPath: function () {
            var selWidget = this._tabContainer.get("selectedChildWidget");
            if (!selWidget || selWidget == this._tabContainer) {
                return null;
            }
            if (selWidget.getPath) {
                return selWidget.getPath();
            }
            return this.idToPath(selWidget.id);
        },

        //  Tab Helpers ---
        getSelectedChild: function () {
            return this._tabContainer.get("selectedChildWidget");
        },

        getChildren: function () {
            return this._tabContainer.getChildren();
        },

        addChild: function (child, pos) {
            //this.disableHashing++;
            var retVal = this._tabContainer.addChild(child, pos);
            //this.disableHashing--;
            return retVal;
        },

        removeChild: function (child) {
            this._tabContainer.removeChild(child);
            child.destroyRecursive();
        },

        removeAllChildren: function() {
            var tabs = this._tabContainer.getChildren();
            for (var i = 0; i < tabs.length; ++i) {
                this.removeChild(tabs[i]);
            }
        },

        selectChild: function (child, doHash) {
            if (!doHash) {
                this.disableHashing++;
            }
            var currSel = this.getSelectedChild();
            if (currSel != child) {
                var nodeExists = dom.byId(child);
                if (nodeExists) {
                    this._tabContainer.selectChild(child);
                }
            } else {
                this.onNewTabSelection({
                    oldWidget: null,
                    newWidget: child
                })
            }
            if (!doHash) {
                this.disableHashing--;
            }
        }
    });
});
