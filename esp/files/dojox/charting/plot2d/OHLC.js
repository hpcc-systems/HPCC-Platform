//>>built
define("dojox/charting/plot2d/OHLC",["dojo/_base/lang","dojo/_base/array","dojo/_base/declare","dojo/has","./CartesianBase","./_PlotEvents","./common","dojox/lang/functional","dojox/lang/functional/reversed","dojox/lang/utils","dojox/gfx/fx"],function(_1,_2,_3,_4,_5,_6,dc,df,_7,du,fx){
var _8=_7.lambda("item.purgeGroup()");
return _3("dojox.charting.plot2d.OHLC",[_5,_6],{defaultParams:{gap:2,animate:null},optionalParams:{minBarSize:1,maxBarSize:1,stroke:{},outline:{},shadow:{},fill:{},font:"",fontColor:""},constructor:function(_9,_a){
this.opt=_1.clone(this.defaultParams);
du.updateWithObject(this.opt,_a);
du.updateWithPattern(this.opt,_a,this.optionalParams);
this.animate=this.opt.animate;
},collectStats:function(_b){
var _c=_1.delegate(dc.defaultStats);
for(var i=0;i<_b.length;i++){
var _d=_b[i];
if(!_d.data.length){
continue;
}
var _e=_c.vmin,_f=_c.vmax;
if(!("ymin" in _d)||!("ymax" in _d)){
_2.forEach(_d.data,function(val,idx){
if(val!==null){
var x=val.x||idx+1;
_c.hmin=Math.min(_c.hmin,x);
_c.hmax=Math.max(_c.hmax,x);
_c.vmin=Math.min(_c.vmin,val.open,val.close,val.high,val.low);
_c.vmax=Math.max(_c.vmax,val.open,val.close,val.high,val.low);
}
});
}
if("ymin" in _d){
_c.vmin=Math.min(_e,_d.ymin);
}
if("ymax" in _d){
_c.vmax=Math.max(_f,_d.ymax);
}
}
return _c;
},getSeriesStats:function(){
var _10=this.collectStats(this.series);
_10.hmin-=0.5;
_10.hmax+=0.5;
return _10;
},render:function(dim,_11){
if(this.zoom&&!this.isDataDirty()){
return this.performZoom(dim,_11);
}
this.resetEvents();
this.dirty=this.isDirty();
if(this.dirty){
_2.forEach(this.series,_8);
this._eventSeries={};
this.cleanGroup();
var s=this.getGroup();
df.forEachRev(this.series,function(_12){
_12.cleanGroup(s);
});
}
var t=this.chart.theme,f,gap,_13,ht=this._hScaler.scaler.getTransformerFromModel(this._hScaler),vt=this._vScaler.scaler.getTransformerFromModel(this._vScaler),_14=this.events();
f=dc.calculateBarSize(this._hScaler.bounds.scale,this.opt);
gap=f.gap;
_13=f.size;
for(var i=this.series.length-1;i>=0;--i){
var run=this.series[i];
if(!this.dirty&&!run.dirty){
t.skip();
this._reconnectEvents(run.name);
continue;
}
run.cleanGroup();
var _15=t.next("candlestick",[this.opt,run]),s=run.group,_16=new Array(run.data.length);
for(var j=0;j<run.data.length;++j){
var v=run.data[j];
if(v!==null){
var _17=t.addMixin(_15,"candlestick",v,true);
var x=ht(v.x||(j+0.5))+_11.l+gap,y=dim.height-_11.b,_18=vt(v.open),_19=vt(v.close),_1a=vt(v.high),low=vt(v.low);
if(low>_1a){
var tmp=_1a;
_1a=low;
low=tmp;
}
if(_13>=1){
var hl={x1:_13/2,x2:_13/2,y1:y-_1a,y2:y-low},op={x1:0,x2:((_13/2)+((_17.series.stroke.width||1)/2)),y1:y-_18,y2:y-_18},cl={x1:((_13/2)-((_17.series.stroke.width||1)/2)),x2:_13,y1:y-_19,y2:y-_19};
var _1b=s.createGroup();
_1b.setTransform({dx:x,dy:0});
var _1c=_1b.createGroup();
_1c.createLine(hl).setStroke(_17.series.stroke);
_1c.createLine(op).setStroke(_17.series.stroke);
_1c.createLine(cl).setStroke(_17.series.stroke);
run.dyn.stroke=_17.series.stroke;
if(_14){
var o={element:"candlestick",index:j,run:run,shape:_1c,x:x,y:y-Math.max(_18,_19),cx:_13/2,cy:(y-Math.max(_18,_19))+(Math.max(_18>_19?_18-_19:_19-_18,1)/2),width:_13,height:Math.max(_18>_19?_18-_19:_19-_18,1),data:v};
this._connectEvents(o);
_16[j]=o;
}
}
if(this.animate){
this._animateOHLC(_1b,y-low,_1a-low);
}
}
}
this._eventSeries[run.name]=_16;
run.dirty=false;
}
this.dirty=false;
if(_4("dojo-bidi")){
this._checkOrientation(this.group,dim,_11);
}
return this;
},_animateOHLC:function(_1d,_1e,_1f){
fx.animateTransform(_1.delegate({shape:_1d,duration:1200,transform:[{name:"translate",start:[0,_1e-(_1e/_1f)],end:[0,0]},{name:"scale",start:[1,1/_1f],end:[1,1]},{name:"original"}]},this.animate)).play();
}});
});
