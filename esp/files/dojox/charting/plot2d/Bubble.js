//>>built
define("dojox/charting/plot2d/Bubble",["dojo/_base/lang","dojo/_base/declare","dojo/_base/array","dojo/has","./CartesianBase","./_PlotEvents","./common","dojox/lang/functional","dojox/lang/functional/reversed","dojox/lang/utils","dojox/gfx/fx"],function(_1,_2,_3,_4,_5,_6,dc,df,_7,du,fx){
var _8=_7.lambda("item.purgeGroup()");
return _2("dojox.charting.plot2d.Bubble",[_5,_6],{defaultParams:{animate:null},optionalParams:{stroke:{},outline:{},shadow:{},fill:{},filter:{},styleFunc:null,font:"",fontColor:"",labelFunc:null},constructor:function(_9,_a){
this.opt=_1.clone(_1.mixin(this.opt,this.defaultParams));
du.updateWithObject(this.opt,_a);
du.updateWithPattern(this.opt,_a,this.optionalParams);
if(!this.opt.labelFunc){
this.opt.labelFunc=function(_b,_c,_d){
return this._getLabel(_b.size,_c,_d);
};
}
this.animate=this.opt.animate;
},render:function(_e,_f){
var s;
if(this.zoom&&!this.isDataDirty()){
return this.performZoom(_e,_f);
}
this.resetEvents();
this.dirty=this.isDirty();
if(this.dirty){
_3.forEach(this.series,_8);
this._eventSeries={};
this.cleanGroup();
s=this.getGroup();
df.forEachRev(this.series,function(_10){
_10.cleanGroup(s);
});
}
var t=this.chart.theme,ht=this._hScaler.scaler.getTransformerFromModel(this._hScaler),vt=this._vScaler.scaler.getTransformerFromModel(this._vScaler),_11=this.events();
for(var i=this.series.length-1;i>=0;--i){
var run=this.series[i];
if(!this.dirty&&!run.dirty){
t.skip();
this._reconnectEvents(run.name);
continue;
}
run.cleanGroup();
if(!run.data.length){
run.dirty=false;
t.skip();
continue;
}
if(typeof run.data[0]=="number"){
console.warn("dojox.charting.plot2d.Bubble: the data in the following series cannot be rendered as a bubble chart; ",run);
continue;
}
s=run.group;
var _12=t.next("circle",[this.opt,run]),_13=_3.map(run.data,function(v){
return v?{x:ht(v.x)+_f.l,y:_e.height-_f.b-vt(v.y),radius:this._vScaler.bounds.scale*(v.size/2)}:null;
},this);
var _14=null,_15=null,_16=null,_17=this.opt.styleFunc;
var _18=function(_19){
if(_17){
return t.addMixin(_12,"circle",[_19,_17(_19)],true);
}
return t.addMixin(_12,"circle",_19,true);
};
if(_12.series.shadow){
_16=_3.map(_13,function(_1a,i){
if(_1a!==null){
var _1b=_18(run.data[i]),_1c=_1b.series.shadow;
var _1d=s.createCircle({cx:_1a.x+_1c.dx,cy:_1a.y+_1c.dy,r:_1a.radius}).setStroke(_1c).setFill(_1c.color);
if(this.animate){
this._animateBubble(_1d,_e.height-_f.b,_1a.radius);
}
return _1d;
}
return null;
},this);
if(_16.length){
run.dyn.shadow=_16[_16.length-1].getStroke();
}
}
if(_12.series.outline){
_15=_3.map(_13,function(_1e,i){
if(_1e!==null){
var _1f=_18(run.data[i]),_20=dc.makeStroke(_1f.series.outline);
_20.width=2*_20.width+_12.series.stroke.width;
var _21=s.createCircle({cx:_1e.x,cy:_1e.y,r:_1e.radius}).setStroke(_20);
if(this.animate){
this._animateBubble(_21,_e.height-_f.b,_1e.radius);
}
return _21;
}
return null;
},this);
if(_15.length){
run.dyn.outline=_15[_15.length-1].getStroke();
}
}
_14=_3.map(_13,function(_22,i){
if(_22!==null){
var _23=_18(run.data[i]),_24={x:_22.x-_22.radius,y:_22.y-_22.radius,width:2*_22.radius,height:2*_22.radius};
var _25=this._plotFill(_23.series.fill,_e,_f);
_25=this._shapeFill(_25,_24);
var _26=s.createCircle({cx:_22.x,cy:_22.y,r:_22.radius}).setFill(_25).setStroke(_23.series.stroke);
if(_26.setFilter&&_23.series.filter){
_26.setFilter(_23.series.filter);
}
if(this.animate){
this._animateBubble(_26,_e.height-_f.b,_22.radius);
}
this.createLabel(s,run.data[i],_24,_23);
return _26;
}
return null;
},this);
if(_14.length){
run.dyn.fill=_14[_14.length-1].getFill();
run.dyn.stroke=_14[_14.length-1].getStroke();
}
if(_11){
var _27=new Array(_14.length);
_3.forEach(_14,function(s,i){
if(s!==null){
var o={element:"circle",index:i,run:run,shape:s,outline:_15&&_15[i]||null,shadow:_16&&_16[i]||null,x:run.data[i].x,y:run.data[i].y,r:run.data[i].size/2,cx:_13[i].x,cy:_13[i].y,cr:_13[i].radius};
this._connectEvents(o);
_27[i]=o;
}
},this);
this._eventSeries[run.name]=_27;
}else{
delete this._eventSeries[run.name];
}
run.dirty=false;
}
this.dirty=false;
if(_4("dojo-bidi")){
this._checkOrientation(this.group,_e,_f);
}
return this;
},_animateBubble:function(_28,_29,_2a){
fx.animateTransform(_1.delegate({shape:_28,duration:1200,transform:[{name:"translate",start:[0,_29],end:[0,0]},{name:"scale",start:[0,1/_2a],end:[1,1]},{name:"original"}]},this.animate)).play();
}});
});
