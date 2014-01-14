//>>built
define("dojox/calendar/StoreMixin",["dojo/_base/declare","dojo/_base/array","dojo/_base/html","dojo/_base/lang","dojo/dom-class","dojo/Stateful","dojo/when"],function(_1,_2,_3,_4,_5,_6,_7){
return _1("dojox.calendar.StoreMixin",_6,{store:null,query:{},queryOptions:null,startTimeAttr:"startTime",endTimeAttr:"endTime",summaryAttr:"summary",allDayAttr:"allDay",cssClassFunc:null,decodeDate:null,encodeDate:null,displayedItemsInvalidated:false,itemToRenderItem:function(_8,_9){
if(this.owner){
return this.owner.itemToRenderItem(_8,_9);
}
return {id:_9.getIdentity(_8),summary:_8[this.summaryAttr],startTime:(this.decodeDate&&this.decodeDate(_8[this.startTimeAttr]))||this.newDate(_8[this.startTimeAttr],this.dateClassObj),endTime:(this.decodeDate&&this.decodeDate(_8[this.endTimeAttr]))||this.newDate(_8[this.endTimeAttr],this.dateClassObj),allDay:_8[this.allDayAttr]!=null?_8[this.allDayAttr]:false,cssClass:this.cssClassFunc?this.cssClassFunc(_8):null};
},renderItemToItem:function(_a,_b){
if(this.owner){
return this.owner.renderItemToItem(_a,_b);
}
var _c={};
_c[_b.idProperty]=_a.id;
_c[this.summaryAttr]=_a.summary;
_c[this.startTimeAttr]=(this.encodeDate&&this.encodeDate(_a.startTime))||_a.startTime;
_c[this.endTimeAttr]=(this.encodeDate&&this.encodeDate(_a.endTime))||_a.endTime;
return this.getItemStoreState(_a)=="unstored"?_c:_4.mixin(_a._item,_c);
},_computeVisibleItems:function(_d){
var _e=_d.startTime;
var _f=_d.endTime;
if(this.items){
_d.items=_2.filter(this.items,function(_10){
return this.isOverlapping(_d,_10.startTime,_10.endTime,_e,_f);
},this);
}
},_initItems:function(_11){
this.set("items",_11);
return _11;
},_refreshItemsRendering:function(_12){
},_updateItems:function(_13,_14,_15){
var _16=true;
var _17=null;
var _18=this.itemToRenderItem(_13,this.store);
_18._item=_13;
if(_14!=-1){
if(_15!=_14){
this.items.splice(_14,1);
if(this.setItemSelected&&this.isItemSelected(_18)){
this.setItemSelected(_18,false);
this.dispatchChange(_18,this.get("selectedItem"),null,null);
}
}else{
_17=this.items[_14];
var cal=this.dateModule;
_16=cal.compare(_18.startTime,_17.startTime)!=0||cal.compare(_18.endTime,_17.endTime)!=0;
_4.mixin(_17,_18);
}
}else{
if(_15!=-1){
var _19=_13.temporaryId;
if(_19){
var l=this.items.length;
for(var i=l-1;i>=0;i--){
if(this.items[i].id==_19){
this.items[i]=_18;
break;
}
}
this._cleanItemStoreState(_19);
this._setItemStoreState(_18,"storing");
}
var s=this._getItemStoreStateObj(_18);
if(s){
if(this.items[_15].id!=_18.id){
var l=this.items.length;
for(var i=l-1;i>=0;i--){
if(this.items[i].id==_18.id){
this.items.splice(i,1);
break;
}
}
this.items.splice(_15,0,_18);
}
_4.mixin(s.renderItem,_18);
}else{
this.items.splice(_15,0,_18);
}
this.set("items",this.items);
}
}
this._setItemStoreState(_18,"stored");
if(!this._isEditing){
if(_16){
this._refreshItemsRendering();
}else{
this.updateRenderers(_17);
}
}
},_setStoreAttr:function(_1a){
this.displayedItemsInvalidated=true;
var r;
if(this._observeHandler){
this._observeHandler.remove();
this._observeHandler=null;
}
if(_1a){
var _1b=_1a.query(this.query,this.queryOptions);
if(_1b.observe){
this._observeHandler=_1b.observe(_4.hitch(this,this._updateItems),true);
}
_1b=_1b.map(_4.hitch(this,function(_1c){
var _1d=this.itemToRenderItem(_1c,_1a);
_1d._item=_1c;
return _1d;
}));
r=_7(_1b,_4.hitch(this,this._initItems));
}else{
r=this._initItems([]);
}
this._set("store",_1a);
return r;
},_getItemStoreStateObj:function(_1e){
if(this.owner){
return this.owner._getItemStoreStateObj(_1e);
}
var _1f=this.get("store");
if(_1f!=null&&this._itemStoreState!=null){
var id=_1e.id==undefined?_1f.getIdentity(_1e):_1e.id;
return this._itemStoreState[id];
}
return null;
},getItemStoreState:function(_20){
if(this.owner){
return this.owner.getItemStoreState(_20);
}
if(this._itemStoreState==null){
return "stored";
}
var _21=this.get("store");
var id=_20.id==undefined?_21.getIdentity(_20):_20.id;
var s=this._itemStoreState[id];
if(_21!=null&&s!=undefined){
return s.state;
}
return "stored";
},_setItemStoreState:function(_22,_23){
if(this.owner){
this.owner._setItemStoreState(_22,_23);
return;
}
if(this._itemStoreState==undefined){
this._itemStoreState={};
}
var _24=this.get("store");
var id=_22.id==undefined?_24.getIdentity(_22):_22.id;
var s=this._itemStoreState[id];
if(_23=="stored"||_23==null){
if(s!=undefined){
delete this._itemStoreState[id];
}
return;
}
if(_24){
this._itemStoreState[id]={id:id,item:_22,renderItem:this.itemToRenderItem(_22,_24),state:_23};
}
},_cleanItemStoreState:function(id){
if(this.owner){
return this.owner._cleanItemStoreState(id);
}
var s=this._itemStoreState[id];
if(s){
delete this._itemStoreState[id];
return true;
}
return false;
}});
});
