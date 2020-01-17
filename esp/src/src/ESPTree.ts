import * as arrayUtil from "dojo/_base/array";
import * as declare from "dojo/_base/declare";
import * as Memory from "dojo/store/Memory";

import * as ESPUtil from "./ESPUtil";

const TreeItem = declare([ESPUtil.Singleton], {
    __hpcc_type: "none",

    constructor(args) {
        args.__hpcc_id = this.__hpcc_type + "::" + args.__hpcc_id;  //  args get set to "this" in base class Stateful ---
    },

    getUniqueID() {
        return this.__hpcc_id;
    },

    getIcon() {
        return "file.png";
    },

    getLabel() {
        return "TODO";
    }
});

const TreeNode = declare(null, {
    treeSeparator: "->",
    constructor(store, parentNode, treeItem) {
        this.__hpcc_store = store;
        if (!(parentNode === null || parentNode instanceof TreeNode)) {
            throw new Error("Invalid Parent Node");
        }
        if (parentNode) {
            parentNode.appendChild(this);
        }
        this.__hpcc_treeItem = treeItem;
        this.__hpcc_id = (this.__hpcc_parentNode ? (this.__hpcc_parentNode.getUniqueID() + this.treeSeparator) : "") + this.__hpcc_treeItem.getUniqueID();
        this.__hpcc_children = [];
    },
    getUniqueID() {
        return this.__hpcc_id;
    },
    mayHaveChildren() {
        return this.__hpcc_children.length;
    },
    appendChild(child) {
        if (!(child instanceof TreeNode)) {
            throw new Error("Invalid Child Node");
        }
        child.__hpcc_parentNode = this;
        this.__hpcc_children.push(child);
    },
    appendChildren(children) {
        arrayUtil.forEach(children, function (child) {
            this.appendChild(child);
        }, this);
    },
    getChildren(options) {
        return this.__hpcc_children;
    },
    getIcon() {
        return this.__hpcc_treeItem.getIcon();
    },
    getLabel() {
        return this.__hpcc_treeItem.getLabel();
    }
});

const TreeStore = declare([Memory], {
    idProperty: "__hpcc_id",
    treeSeparator: "->",

    constructor(args) {
        this.clear();
    },

    clear() {
        this.cachedTreeNodes = {};
    },

    setRootNode(rootItem) {
        const rootNode = this.createTreeNode(null, rootItem);
        this.setData([rootNode]);
        return rootNode;
    },

    createTreeNode(parentNode, treeItem) {
        const retVal = new TreeNode(this, parentNode, treeItem);
        if (this.cachedTreeNodes[retVal.getUniqueID()]) {
            // throw retVal.getUniqueID() + " already exists.";
        }
        this.cachedTreeNodes[retVal.getUniqueID()] = retVal;
        return retVal;
    },

    addItem(treeItem) {
        if (this.cacheTreeItems[treeItem.getUniqueID()]) {
            throw new Error(treeItem.getUniqueID() + " already exists.");
        }
        treeItem.__hpcc_store = this;
        this.cacheTreeItems[treeItem.getUniqueID()] = treeItem;
        return treeItem;
    },

    addChild(source, target) {
        this.out_edges[source.getUniqueID()].put(this.createTreeNode(source, target));
        this.in_edges[target.getUniqueID()].put(this.createTreeNode(target, source));
        return target;
    },

    addChildren(source, targets) {
        arrayUtil.forEach(targets, function (target) {
            this.addChild(source, target);
        }, this);
    },

    mayHaveChildren(treeNode) {
        return treeNode.mayHaveChildren && treeNode.mayHaveChildren();
    },

    get(id) {
        return this.cachedTreeNodes[id];
    },

    getChildren(parent, options) {
        return parent.getChildren(options);
    }
});

export const Store = TreeStore;
export const Item = TreeItem;
