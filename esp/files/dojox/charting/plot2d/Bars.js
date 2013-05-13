//>>built
define("dojox/charting/plot2d/Bars",["dojo/_base/lang","dojo/_base/array","dojo/_base/declare","dojo/has","./CartesianBase","./_PlotEvents","./common","dojox/gfx/fx","dojox/lang/utils","dojox/lang/functional","dojox/lang/functional/reversed"],function(_1,_2,_3,_4,_5,_6,dc,fx,du,df,_7){
var _8=_7.lambda("item.purgeGroup()");
return _3("dojox.charting.plot2d.Bars",[_5,_6],{defaultParams:{gap:0,animate:null,enableCache:false},optionalParams:{minBarSize:1,maxBarSize:1,stroke:{},outline:{},shadow:{},fill:{},filter:{},styleFunc:null,font:"",fontColor:""},constructor:function(_9,_a){
this.opt=_1.clone(_1.mixin(this.opt,this.defaultParams));
du.updateWithObject(this.opt,_a);
du.updateWithPattern(this.opt,_a,this.optionalParams);
this.animate=this.opt.animate;
},getSeriesStats:function(){
var _b=dc.collectSimpleStats(this.series),t;
_b.hmin-=0.5;
_b.hmax+=0.5;
t=_b.hmin,_b.hmin=_b.vmin,_b.vmin=t;
t=_b.hmax,_b.hmax=_b.vmax,_b.vmax=t;
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
},createLabel:function(_10,_11,_12,_13){
if(this.opt.labels&&this.opt.labelStyle=="outside"){
var y=_12.y+_12.height/2;
var x=_12.x+_12.width+this.opt.labelOffset;
this.renderLabel(_10,x,y,this._getLabel(isNaN(_11.y)?_11:_11.y),_13,"start");
}else{
this.inherited(arguments);
}
},render:function(dim,_14){
if(this.zoom&&!this.isDataDirty()){
return this.performZoom(dim,_14);
}
this.dirty=this.isDirty();
this.resetEvents();
var s;
if(this.dirty){
_2.forEach(this.series,_8);
this._eventSeries={};
this.cleanGroup();
s=this.getGroup();
df.forEachRev(this.series,function(_15){
_15.cleanGroup(s);
});
}
var t=this.chart.theme,ht=this._hScaler.scaler.getTransformerFromModel(this._hScaler),vt=this._vScaler.scaler.getTransformerFromModel(this._vScaler),_16=Math.max(0,this._hScaler.bounds.lower),_17=ht(_16),_18=this.events();
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
var _19=t.next("bar",[this.opt,run]),_1a=new Array(run.data.length);
s=run.group;
var _1b=_2.some(run.data,function(_1c){
return typeof _1c=="number"||(_1c&&!_1c.hasOwnProperty("x"));
});
var min=_1b?Math.max(0,Math.floor(this._vScaler.bounds.from-1)):0;
var max=_1b?Math.min(run.data.length,Math.ceil(this._vScaler.bounds.to)):run.data.length;
for(var j=min;j<max;++j){
var _1d=run.data[j];
if(_1d!=null){
var val=this.getValue(_1d,j,i,_1b),hv=ht(val.y),w=Math.abs(hv-_17),_1e,_1f;
if(this.opt.styleFunc||typeof _1d!="number"){
var _20=typeof _1d!="number"?[_1d]:[];
if(this.opt.styleFunc){
_20.push(this.opt.styleFunc(_1d));
}
_1e=t.addMixin(_19,"bar",_20,true);
}else{
_1e=t.post(_19,"bar");
}
if(w>=0&&bar.height>=1){
var _21={x:_14.l+(val.y<_16?hv:_17),y:dim.height-_14.b-vt(val.x+1.5)+bar.gap+bar.thickness*(this.series.length-i-1),width:w,height:bar.height};
if(_1e.series.shadow){
var _22=_1.clone(_21);
_22.x+=_1e.series.shadow.dx;
_22.y+=_1e.series.shadow.dy;
_1f=this.createRect(run,s,_22).setFill(_1e.series.shadow.color).setStroke(_1e.series.shadow);
if(this.animate){
this._animateBar(_1f,_14.l+_17,-w);
}
}
var _23=this._plotFill(_1e.series.fill,dim,_14);
_23=this._shapeFill(_23,_21);
var _24=this.createRect(run,s,_21).setFill(_23).setStroke(_1e.series.stroke);
if(_24.setFilter&&_1e.series.filter){
_24.setFilter(_1e.series.filter);
}
run.dyn.fill=_24.getFill();
run.dyn.stroke=_24.getStroke();
if(_18){
var o={element:"bar",index:j,run:run,shape:_24,shadow:_1f,cx:val.y,cy:val.x+1.5,x:_1b?j:run.data[j].x,y:_1b?run.data[j]:run.data[j].y};
this._connectEvents(o);
_1a[j]=o;
}
if(!isNaN(val.py)&&val.py>_16){
_21.x+=ht(val.py);
_21.width-=ht(val.py);
}
this.createLabel(s,_1d,_21,_1e);
if(this.animate){
this._animateBar(_24,_14.l+_17,-w);
}
}
}
}
this._eventSeries[run.name]=_1a;
run.dirty=false;
}
this.dirty=false;
if(_4("dojo-bidi")){
this._checkOrientation(this.group,dim,_14);
}
return this;
},getValue:function(_25,j,_26,_27){
var y,x;
if(_27){
if(typeof _25=="number"){
y=_25;
}else{
y=_25.y;
}
x=j;
}else{
y=_25.y;
x=_25.x-1;
}
return {y:y,x:x};
},getBarProperties:function(){
var f=dc.calculateBarSize(this._vScaler.bounds.scale,this.opt);
return {gap:f.gap,height:f.size,thickness:0};
},_animateBar:function(_28,_29,_2a){
if(_2a==0){
_2a=1;
}
fx.animateTransform(_1.delegate({shape:_28,duration:1200,transform:[{name:"translate",start:[_29-(_29/_2a),0],end:[0,0]},{name:"scale",start:[1/_2a,1],end:[1,1]},{name:"original"}]},this.animate)).play();
}});
});
