//>>built
define("dojox/mobile/_StoreListMixin",["dojo/_base/array","dojo/_base/declare","./_StoreMixin","./ListItem","dojo/has","dojo/has!dojo-bidi?dojox/mobile/bidi/_StoreListMixin"],function(_1,_2,_3,_4,_5,_6){
var _7=_2(_5("dojo-bidi")?"dojox.mobile._NonBidiStoreListMixin":"dojox.mobile._StoreListMixin",_3,{append:false,itemMap:null,itemRenderer:_4,buildRendering:function(){
this.inherited(arguments);
if(!this.store){
return;
}
var _8=this.store;
this.store=null;
this.setStore(_8,this.query,this.queryOptions);
},createListItem:function(_9){
var _a={};
if(!_9["label"]){
_a["label"]=_9[this.labelProperty];
}
if(_5("dojo-bidi")&&typeof _a["dir"]=="undefined"){
_a["dir"]=this.isLeftToRight()?"ltr":"rtl";
}
for(var _b in _9){
_a[(this.itemMap&&this.itemMap[_b])||_b]=_9[_b];
}
return new this.itemRenderer(_a);
},_setDirAttr:function(_c){
return _c;
},generateList:function(_d){
if(!this.append){
_1.forEach(this.getChildren(),function(_e){
_e.destroyRecursive();
});
}
_1.forEach(_d,function(_f,_10){
this.addChild(this.createListItem(_f));
if(_f[this.childrenProperty]){
_1.forEach(_f[this.childrenProperty],function(_11,_12){
this.addChild(this.createListItem(_11));
},this);
}
},this);
},onComplete:function(_13){
this.generateList(_13);
},onError:function(){
},onAdd:function(_14,_15){
this.addChild(this.createListItem(_14),_15);
},onUpdate:function(_16,_17){
this.getChildren()[_17].set(_16);
},onDelete:function(_18,_19){
this.getChildren()[_19].destroyRecursive();
}});
return _5("dojo-bidi")?_2("dojox.mobile._StoreListMixin",[_7,_6]):_7;
});
