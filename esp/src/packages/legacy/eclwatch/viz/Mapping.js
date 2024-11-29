define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/_base/array"
], function (declare, lang, arrayUtil) {
    var Entity = declare(null, {
        _id: null,
        _display: null,
        _attrs: null,

        constructor: function (id, display) {
            this._id = id;
            this._display = display;
            this._attrs = {};
        },

        getID: function () {
            return this._id;
        },

        getDisplay: function () {
            return this._display.split(" ").join("&nbsp;");
        },

        setAttr: function (attr, val) {
            this._attrs[attr] = val;
        },

        getAttr: function (attr) {
            return this._attrs[attr];
        }
    });

    var EntityArray = declare(null, {
        entities: null,

        constructor: function (id, display) {
            this.entities = {};
        },

        add: function (entity) {
            this.entities[entity.getID()] = entity;
            return entity;
        },

        getOne: function () {
            for (var key in this.entities) {
                return this.entities[key];
            }
            return null;
        },

        get: function (id) {
            if (id) {
                return this.entities[id];
            }
            return this.getOne();
        },

        getAll: function () {
            return this.entities;
        },

        getArray: function () {
            var retVal = [];
            for (var key in this.entities) {
                retVal.push(this.entities[key]);
            }
            return retVal;
        },

        setAttr: function (id, attr, val) {
            this.entities[id].setAttr(attr, val);
        },

        getAttr: function (id, attr) {
            return this.entities[id].getAttr(attr);
        }
    });

    var Field = declare(Entity, {
        constructor: function () {
        }
    });

    var Dataset = declare(Entity, {
        fields: null,

        constructor: function (id, display, fields) {
            this.fields = new EntityArray();
            for (var key in fields) {
                this.addField(key, fields[key]);
            }
        },

        addField: function (id, display) {
            return this.fields.add(new Field(id, display));
        },

        setFieldMapping: function (id, field) {
            this.fields.setAttr(id, "field", field);
        },

        getFieldMapping: function (id) {
            return this.fields.getAttr(id, "field");
        },

        getFieldMappings: function (id) {
            return this.fields.getArray();
        },

        setData: function (data) {
            this.data = data;
        },

        getMappedData: function () {
            return this._mapArray(this.data);
        },

        hasData: function () {
            if (this.data && this.data.length) {
                return true;
            }
        },

        _mapItem: function (item) {
            if (!item)
                return item;

            var retVal = {};
            for (var key in this.fields.getAll()) {
                var field = this.fields.getAttr(key, "field");
                var val;
                if (field && lang.exists(field, item)) {
                    val = item[field];
                } else {
                    val = item[key];
                }
                if (val === null || val === undefined) {
                } else if (Object.prototype.toString.call(val) === "[object Array]") {
                    retVal[key] = this.delegateArray(val);
                } else if (!isNaN(parseFloat(val))) {
                    retVal[key] = parseFloat(val);
                } else {
                    retVal[key] = val.trim();
                }
            }
            return retVal;
        },

        _mapArray: function (arr) {
            if (!arr)
                return arr;

            return arr.map(lang.hitch(this, function (item) {
                return this._mapItem(item);
            }));
        }
    });

    return declare(null, {
        datasets: null,

        constructor: function () {
            this.datasets = new EntityArray();
            this.setDatasetMappings(this.mapping);
        },

        //  Datasets  ---
        setDatasetMappings: function (datasets) {
            for (var key in datasets) {
                this.setDatasetMapping(key, datasets[key].display, datasets[key].fields);
            }
        },

        setDatasetMapping: function (id, display, fields) {
            return this.datasets.add(new Dataset(id, display, fields));
        },

        getDatasetMappings: function () {
            return this.datasets.getArray();
        },

        cloneDatasetMappings: function () {
            return lang.clone(this.datasets.getArray());
        },

        //  Fields  ---
        setFieldMappings: function (mappings, datasetID) {
            var dataset = this.datasets.get(datasetID);
            for (var key in mappings) {
                dataset.setFieldMapping(key, mappings[key]);
            }
        },

        setFieldMapping: function (id, field, datasetID) {
            var dataset = this.datasets.get(datasetID);
            dataset.setFieldMapping(id, field);
        },

        getFieldMapping: function (id, datasetID) {
            var dataset = this.datasets.get(datasetID);
            return dataset.getFieldMapping(id) || id;
        },

        //  Data  ---
        setData: function (data, datasetID) {
            var dataset = this.datasets.get(datasetID);
            dataset.setData(data);
        },

        getMappedData: function (datasetID) {
            var dataset = this.datasets.get(datasetID);
            return dataset.getMappedData();
        },

        hasData: function () {
            var retVal = false;
            arrayUtil.forEach(this.datasets.getArray(), function (item, idx) {
                retVal = true;
                if (!item.hasData()) {
                    retVal = false;
                    return true;
                }
            });
            return retVal;
        }
    });
});
