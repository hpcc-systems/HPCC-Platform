//>>built
define("dojox/charting/plot2d/Columns",["dojo/_base/lang","dojo/_base/array","dojo/_base/declare","dojo/has","./CartesianBase","./_PlotEvents","./common","dojox/lang/functional","dojox/lang/functional/reversed","dojox/lang/utils","dojox/gfx/fx"],function(_1,_2,_3,_4,_5,_6,dc,df,_7,du,fx){
var _8=_7.lambda("item.purgeGroup()");
return _3("dojox.charting.plot2d.Columns",[_5,_6],{defaultParams:{gap:0,animate:null,enableCache:false},optionalParams:{minBarSize:1,maxBarSize:1,stroke:{},outline:{},shadow:{},fill:{},filter:{},styleFunc:null,font:"",fontColor:""},constructor:function(_9,_a){
this.opt=_1.clone(_1.mixin(this.opt,this.defaultParams));
du.updateWithObject(this.opt,_a);
du.updateWithPattern(this.opt,_a,this.optionalParams);
this.animate=this.opt.animate;
},getSeriesStats:function(){
var _b=dc.collectSimpleStats(this.series);
_b.hmin-=0.5;
_b.hmax+=0.5;
return _b;
},createRect:function(_c,_d,_e){
var _f;
if(this.opt.enableCache&&_c._rectFreePool.length>0){
_f=_c._rectFreePool.pop();
_f.setShape(_e);
_d.add(_f);
}else{
_f=_d.createRect(_e);
}
if(this.opt.enableCache){
_c._rectUsePool.push(_f);
}
return _f;
},render:function(dim,_10){
if(this.zoom&&!this.isDataDirty()){
return this.performZoom(dim,_10);
}
this.resetEvents();
this.dirty=this.isDirty();
var s;
if(this.dirty){
_2.forEach(this.series,_8);
this._eventSeries={};
this.cleanGroup();
s=this.getGroup();
df.forEachRev(this.series,function(_11){
_11.cleanGroup(s);
});
}
var t=this.chart.theme,ht=this._hScaler.scaler.getTransformerFromModel(this._hScaler),vt=this._vScaler.scaler.getTransformerFromModel(this._vScaler),_12=Math.max(0,this._vScaler.bounds.lower),_13=vt(_12),_14=this.events();
var bar=this.getBarProperties();
for(var i=this.series.length-1;i>=0;--i){
var run=this.series[i];
if(!this.dirty&&!run.dirty){
t.skip();
this._reconnectEvents(run.name);
continue;
}
run.cleanGroup();
if(this.opt.enableCache){
run._rectFreePool=(run._rectFreePool?run._rectFreePool:[]).concat(run._rectUsePool?run._rectUsePool:[]);
run._rectUsePool=[];
}
var _15=t.next("column",[this.opt,run]),_16=new Array(run.data.length);
s=run.group;
var _17=_2.some(run.data,function(_18){
return typeof _18=="number"||(_18&&!_18.hasOwnProperty("x"));
});
var min=_17?Math.max(0,Math.floor(this._hScaler.bounds.from-1)):0;
var max=_17?Math.min(run.data.length,Math.ceil(this._hScaler.bounds.to)):run.data.length;
for(var j=min;j<max;++j){
var _19=run.data[j];
if(_19!=null){
var val=this.getValue(_19,j,i,_17),vv=vt(val.y),h=Math.abs(vv-_13),_1a,_1b;
if(this.opt.styleFunc||typeof _19!="number"){
var _1c=typeof _19!="number"?[_19]:[];
if(this.opt.styleFunc){
_1c.push(this.opt.styleFunc(_19));
}
_1a=t.addMixin(_15,"column",_1c,true);
}else{
_1a=t.post(_15,"column");
}
if(bar.width>=1&&h>=0){
var _1d={x:_10.l+ht(val.x+0.5)+bar.gap+bar.thickness*i,y:dim.height-_10.b-(val.y>_12?vv:_13),width:bar.width,height:h};
if(_1a.series.shadow){
var _1e=_1.clone(_1d);
_1e.x+=_1a.series.shadow.dx;
_1e.y+=_1a.series.shadow.dy;
_1b=this.createRect(run,s,_1e).setFill(_1a.series.shadow.color).setStroke(_1a.series.shadow);
if(this.animate){
this._animateColumn(_1b,dim.height-_10.b+_13,h);
}
}
var _1f=this._plotFill(_1a.series.fill,dim,_10);
_1f=this._shapeFill(_1f,_1d);
var _20=this.createRect(run,s,_1d).setFill(_1f).setStroke(_1a.series.stroke);
if(_20.setFilter&&_1a.series.filter){
_20.setFilter(_1a.series.filter);
}
run.dyn.fill=_20.getFill();
run.dyn.stroke=_20.getStroke();
if(_14){
var o={element:"column",index:j,run:run,shape:_20,shadow:_1b,cx:val.x+0.5,cy:val.y,x:_17?j:run.data[j].x,y:_17?run.data[j]:run.data[j].y};
this._connectEvents(o);
_16[j]=o;
}
if(!isNaN(val.py)&&val.py>_12){
_1d.height=vv-vt(val.py);
}
this.createLabel(s,_19,_1d,_1a);
if(this.animate){
this._animateColumn(_20,dim.height-_10.b-_13,h);
}
}
}
}
this._eventSeries[run.name]=_16;
run.dirty=false;
}
this.dirty=false;
if(_4("dojo-bidi")){
this._checkOrientation(this.group,dim,_10);
}
return this;
},getValue:function(_21,j,_22,_23){
var y,x;
if(_23){
if(typeof _21=="number"){
y=_21;
}else{
y=_21.y;
}
x=j;
}else{
y=_21.y;
x=_21.x-1;
}
return {x:x,y:y};
},getBarProperties:function(){
var f=dc.calculateBarSize(this._hScaler.bounds.scale,this.opt);
return {gap:f.gap,width:f.size,thickness:0};
},_animateColumn:function(_24,_25,_26){
if(_26==0){
_26=1;
}
fx.animateTransform(_1.delegate({shape:_24,duration:1200,transform:[{name:"translate",start:[0,_25-(_25/_26)],end:[0,0]},{name:"scale",start:[1,1/_26],end:[1,1]},{name:"original"}]},this.animate)).play();
}});
});
