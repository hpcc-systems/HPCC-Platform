define([
    "dojo/_base/declare",
    "dojo/store/Memory",
    "dojo/store/Observable",

    "dijit/registry",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/_Widget",

    "dojo/text!../templates/SelectionGridWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane"
], function (declare, Memory, Observable,
    registry,
    OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry,
    _Widget,
    template) {
    return declare("SelectionGridWidget", [_Widget], {
        templateString: template,
        store: null,
        idProperty: "Change Me",

        constructor: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
        },

        resize: function (args) {
            this.inherited(arguments);
            this.borderContainer.resize();
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        //  Implementation ---
        createGrid: function (args) {
            this.idProperty = args.idProperty;
            var store = new Memory({
                idProperty: this.idProperty,
                data: []
            });
            this.store = Observable(store);

            this.grid = new declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry])({
                store: this.store,
                columns: args.columns
            }, this.id + "Grid");
        },

        setData: function (data) {
            this.store.setData(data);
            this.grid.refresh();
        }
    });
});
