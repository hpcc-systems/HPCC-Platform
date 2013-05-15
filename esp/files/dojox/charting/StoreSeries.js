//>>built
define("dojox/charting/StoreSeries",["dojo/_base/array","dojo/_base/declare","dojo/_base/Deferred"],function(_1,_2,_3){
return _2("dojox.charting.StoreSeries",null,{constructor:function(_4,_5,_6){
this.store=_4;
this.kwArgs=_5;
if(_6){
if(typeof _6=="function"){
this.value=_6;
}else{
if(typeof _6=="object"){
this.value=function(_7){
var o={};
for(var _8 in _6){
o[_8]=_7[_6[_8]];
}
return o;
};
}else{
this.value=function(_9){
return _9[_6];
};
}
}
}else{
this.value=function(_a){
return _a.value;
};
}
this.data=[];
this._initialRendering=false;
this.fetch();
},destroy:function(){
if(this.observeHandle){
this.observeHandle.remove();
}
},setSeriesObject:function(_b){
this.series=_b;
},fetch:function(){
var _c=this;
if(this.observeHandle){
this.observeHandle.remove();
}
var _d=this.store.query(this.kwArgs.query,this.kwArgs);
_3.when(_d,function(_e){
_c.objects=_e;
_f();
});
if(_d.observe){
this.observeHandle=_d.observe(_f,true);
}
function _f(){
_c.data=_1.map(_c.objects,function(_10){
return _c.value(_10,_c.store);
});
_c._pushDataChanges();
};
},_pushDataChanges:function(){
if(this.series){
this.series.chart.updateSeries(this.series.name,this,this._initialRendering);
this._initialRendering=false;
this.series.chart.delayedRender();
}
}});
});
