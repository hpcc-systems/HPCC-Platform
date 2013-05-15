//>>built
define("dojox/charting/plot2d/Base",["dojo/_base/declare","dojo/_base/array","dojox/gfx","../Element","./common","../axis2d/common","dojo/has"],function(_1,_2,_3,_4,_5,ac,_6){
var _7=_1("dojox.charting.plot2d.Base",_4,{constructor:function(_8,_9){
if(_9&&_9.tooltipFunc){
this.tooltipFunc=_9.tooltipFunc;
}
},clear:function(){
this.series=[];
this.dirty=true;
return this;
},setAxis:function(_a){
return this;
},assignAxes:function(_b){
_2.forEach(this.axes,function(_c){
if(this[_c]){
this.setAxis(_b[this[_c]]);
}
},this);
},addSeries:function(_d){
this.series.push(_d);
return this;
},getSeriesStats:function(){
return _5.collectSimpleStats(this.series);
},calculateAxes:function(_e){
this.initializeScalers(_e,this.getSeriesStats());
return this;
},initializeScalers:function(){
return this;
},isDataDirty:function(){
return _2.some(this.series,function(_f){
return _f.dirty;
});
},render:function(dim,_10){
return this;
},renderLabel:function(_11,x,y,_12,_13,_14,_15){
var _16=ac.createText[this.opt.htmlLabels&&_3.renderer!="vml"?"html":"gfx"](this.chart,_11,x,y,_15?_15:"middle",_12,_13.series.font,_13.series.fontColor);
if(_14){
if(this.opt.htmlLabels&&_3.renderer!="vml"){
_16.style.pointerEvents="none";
}else{
if(_16.rawNode){
_16.rawNode.style.pointerEvents="none";
}
}
}
if(this.opt.htmlLabels&&_3.renderer!="vml"){
this.htmlElements.push(_16);
}
return _16;
},getRequiredColors:function(){
return this.series.length;
},_getLabel:function(_17){
return _5.getLabel(_17,this.opt.fixed,this.opt.precision);
}});
if(_6("dojo-bidi")){
_7.extend({_checkOrientation:function(_18,dim,_19){
this.chart.applyMirroring(this.group,dim,_19);
}});
}
return _7;
});
