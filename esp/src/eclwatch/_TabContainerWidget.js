define([
    "dojo/_base/declare", 
    "dojo/_base/lang", 
    "dojo/_base/array",
    "dojo/dom",
    "dojo/hash",
    "dojo/router",
    "dojo/aspect",

    "hpcc/_Widget",

    "dijit/registry"
], function (declare, lang, arrayUtil, dom, hash, router, aspect,
    _Widget,
    registry) {

    return declare("_TabContainerWidget", [_Widget], {
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
            var parts = id.split("_");
            return "/" + parts.join("/");
        },

        pathToId: function (path) {
            var obj = path.split("/");
            obj.splice(0, 1);
            return obj.join("_");
        },

        getFirstChildID: function (id) {
            if (id.indexOf(this.id) === 0) {
                var childParts = id.substring(this.id.length).split("_");
                return this.id + "_" + childParts[1];
            }
            return "";
        },

        startsWith: function (tst, str) {
            if (!tst || !str || tst.length > str.length) {
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
            this.inherited(arguments);
            var context = this;
            var d = location;
            aspect.after(this, "init", function (args) {
                this.onNewTabSelection();
                router.register(this.getPath() + "/:sel", function (evt) {
                    context.routerCallback(evt, true);
                });
                router.registerBefore(this.getPath() + "/:sel/*other", function (evt) {
                    context.routerCallback(evt, false);
                });
                router.startup();
            });
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
            if (this.disableHashing) {
                this.go(this.getSelectedPath(), false, true);
            } else {
                var overwrite = this.startsWith(currHash, newHash);
                this.go(this.getSelectedPath(), overwrite);
            }
        },

        go: function (path, replace, noHash) {
            //console.log(this.id + ".go(" + path + ", " + replace + ", " + noHash + ")");
            if (!path)
                return;

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
            if (this.endsWith(newSel, "-DL")) {
                newSel = newSel.substring(0, newSel.length - 3);
            }
            if (!currSel || currSel.id != newSel) {
                this.selectChild(newSel, null);
            } else if (this.initTab) {
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

        getTabChildren: function () {
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

        selectChild: function (childID, doHash) {
            if (!doHash) {
                this.disableHashing++;
            }
            var currSel = this.getSelectedChild();
            var child = registry.byId(childID);
            if (currSel != child) {
                var childIndex = this._tabContainer.getIndexOfChild(child);
                if (childIndex >= 0) {
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
            return child;
        },
        restoreFromHash: function (hash) {
            if (hash) {
                var hashID = this.pathToId(hash);
                var firstChildID = this.getFirstChildID(hashID);
                if (firstChildID) {
                    if (this.endsWith(firstChildID, "-DL")) {
                        firstChildID = firstChildID.substring(0, firstChildID.length - 3);
                    }
                    var child = this.selectChild(firstChildID, false);
                    if (child) {
                        if (this.initTab) {
                            this.initTab();
                        }
                        if (child.restoreFromHash) {
                            child.restoreFromHash(hash);
                        }
                    }
                }
            }
        }
    });
});
