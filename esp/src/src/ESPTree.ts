/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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
    "dojo/_base/array",
    "dojo/store/Memory",
    "dojo/store/Observable",
    "dojo/store/util/QueryResults",

    "hpcc/ESPUtil"
], function (declare, arrayUtil, Memory, Observable, QueryResults,
    ESPUtil) {

    var TreeItem = declare([ESPUtil.Singleton], {
        __hpcc_type: "none",

        constructor: function (args) {
            args.__hpcc_id = this.__hpcc_type + "::" + args.__hpcc_id;  //  args get set to "this" in base class Stateful ---
        },

        getUniqueID: function () {
            return this.__hpcc_id;
        },

        getIcon: function () {
            return "file.png";
        },

        getLabel: function () {
            return "TODO";
        }
    });

    var TreeNode = declare(null, {
        treeSeparator: "->",
        constructor: function (store, parentNode, treeItem) {
            this.__hpcc_store = store;
            if (!(parentNode === null || parentNode instanceof TreeNode)) {
                throw "Invalid Parent Node"
            }
            if (parentNode) {
                parentNode.appendChild(this);
            }
            this.__hpcc_treeItem = treeItem;
            this.__hpcc_id = (this.__hpcc_parentNode ? (this.__hpcc_parentNode.getUniqueID() + this.treeSeparator) : "") + this.__hpcc_treeItem.getUniqueID();
            this.__hpcc_children = [];
        },
        getUniqueID: function () {
            return this.__hpcc_id;
        },
        mayHaveChildren: function () {
            return this.__hpcc_children.length;
        },
        appendChild: function (child) {
            if (!(child instanceof TreeNode)) {
                throw "Invalid Child Node"
            }
            child.__hpcc_parentNode = this;
            this.__hpcc_children.push(child);
        },
        appendChildren: function (children) {
            arrayUtil.forEach(children, function (child) {
                this.appendChild(child);
            }, this);
        },
        getChildren: function (options) {
            return this.__hpcc_children;
        },
        getIcon: function () {
            return this.__hpcc_treeItem.getIcon();
        },
        getLabel: function () {
            return this.__hpcc_treeItem.getLabel();
        }
    });

    var TreeStore = declare([Memory], {
        idProperty: "__hpcc_id",
        treeSeparator: "->",

        constructor: function (args) {
            this.clear();
        },

        clear: function () {
            this.cachedTreeNodes = {};
        },

        setRootNode: function (rootItem) {
            var rootNode = this.createTreeNode(null, rootItem);
            this.setData([rootNode]);
            return rootNode;
        },

        createTreeNode: function (parentNode, treeItem) {
            var retVal = new TreeNode(this, parentNode, treeItem);
            if (this.cachedTreeNodes[retVal.getUniqueID()]) {
                //throw retVal.getUniqueID() + " already exists.";
            }
            this.cachedTreeNodes[retVal.getUniqueID()] = retVal;
            return retVal;
        },

        addItem: function (treeItem) {
            if (this.cacheTreeItems[treeItem.getUniqueID()]) {
                throw treeItem.getUniqueID() + " already exists.";
            }
            treeItem.__hpcc_store = this;
            this.cacheTreeItems[treeItem.getUniqueID()] = treeItem;
            return treeItem;
        },

        addChild: function (source, target) {
            this.out_edges[source.getUniqueID()].put(this.createTreeNode(source, target));
            this.in_edges[target.getUniqueID()].put(this.createTreeNode(target, source));
            return target;
        },

        addChildren: function (source, targets) {
            arrayUtil.forEach(targets, function (target) {
                this.addChild(source, target);
            }, this);
        },

        mayHaveChildren: function (treeNode) {
            return treeNode.mayHaveChildren && treeNode.mayHaveChildren();
        },

        get: function (id) {
            return this.cachedTreeNodes[id];
        },

        getChildren: function (parent, options) {
            return parent.getChildren(options);
        }
    });

    return {
        Store: TreeStore,
        Item: TreeItem
    };
});
