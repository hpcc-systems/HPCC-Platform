//>>built
define("dojox/charting/plot2d/Scatter",["dojo/_base/lang","dojo/_base/array","dojo/_base/declare","dojo/has","./CartesianBase","./_PlotEvents","./common","dojox/lang/functional","dojox/lang/functional/reversed","dojox/lang/utils","dojox/gfx/fx","dojox/gfx/gradutils"],function(_1,_2,_3,_4,_5,_6,dc,df,_7,du,fx,_8){
var _9=_7.lambda("item.purgeGroup()");
return _3("dojox.charting.plot2d.Scatter",[_5,_6],{defaultParams:{shadows:null,animate:null},optionalParams:{markerStroke:{},markerOutline:{},markerShadow:{},markerFill:{},markerFont:"",markerFontColor:"",styleFunc:null},constructor:function(_a,_b){
this.opt=_1.clone(_1.mixin(this.opt,this.defaultParams));
du.updateWithObject(this.opt,_b);
du.updateWithPattern(this.opt,_b,this.optionalParams);
this.animate=this.opt.animate;
},render:function(_c,_d){
if(this.zoom&&!this.isDataDirty()){
return this.performZoom(_c,_d);
}
this.resetEvents();
this.dirty=this.isDirty();
var s;
if(this.dirty){
_2.forEach(this.series,_9);
this._eventSeries={};
this.cleanGroup();
s=this.getGroup();
df.forEachRev(this.series,function(_e){
_e.cleanGroup(s);
});
}
var t=this.chart.theme,_f=this.events();
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
var _10=t.next("marker",[this.opt,run]),_11,ht=this._hScaler.scaler.getTransformerFromModel(this._hScaler),vt=this._vScaler.scaler.getTransformerFromModel(this._vScaler);
s=run.group;
if(typeof run.data[0]=="number"){
_11=_2.map(run.data,function(v,i){
return {x:ht(i+1)+_d.l,y:_c.height-_d.b-vt(v)};
},this);
}else{
_11=_2.map(run.data,function(v,i){
return {x:ht(v.x)+_d.l,y:_c.height-_d.b-vt(v.y)};
},this);
}
var _12=new Array(_11.length),_13=new Array(_11.length),_14=new Array(_11.length);
_2.forEach(_11,function(c,i){
var _15=run.data[i],_16;
if(this.opt.styleFunc||typeof _15!="number"){
var _17=typeof _15!="number"?[_15]:[];
if(this.opt.styleFunc){
_17.push(this.opt.styleFunc(_15));
}
_16=t.addMixin(_10,"marker",_17,true);
}else{
_16=t.post(_10,"marker");
}
var _18="M"+c.x+" "+c.y+" "+_16.symbol;
if(_16.marker.shadow){
_12[i]=s.createPath("M"+(c.x+_16.marker.shadow.dx)+" "+(c.y+_16.marker.shadow.dy)+" "+_16.symbol).setStroke(_16.marker.shadow).setFill(_16.marker.shadow.color);
if(this.animate){
this._animateScatter(_12[i],_c.height-_d.b);
}
}
if(_16.marker.outline){
var _19=dc.makeStroke(_16.marker.outline);
_19.width=2*_19.width+_16.marker.stroke.width;
_14[i]=s.createPath(_18).setStroke(_19);
if(this.animate){
this._animateScatter(_14[i],_c.height-_d.b);
}
}
var _1a=dc.makeStroke(_16.marker.stroke),_1b=this._plotFill(_16.marker.fill,_c,_d);
if(_1b&&(_1b.type==="linear"||_1b.type=="radial")){
var _1c=_8.getColor(_1b,{x:c.x,y:c.y});
if(_1a){
_1a.color=_1c;
}
_13[i]=s.createPath(_18).setStroke(_1a).setFill(_1c);
}else{
_13[i]=s.createPath(_18).setStroke(_1a).setFill(_1b);
}
if(this.opt.labels){
var _1d=_13[i].getBoundingBox();
this.createLabel(s,_15,_1d,_16);
}
if(this.animate){
this._animateScatter(_13[i],_c.height-_d.b);
}
},this);
if(_13.length){
run.dyn.marker=_10.symbol;
run.dyn.markerStroke=_13[_13.length-1].getStroke();
run.dyn.markerFill=_13[_13.length-1].getFill();
}
if(_f){
var _1e=new Array(_13.length);
_2.forEach(_13,function(s,i){
var o={element:"marker",index:i,run:run,shape:s,outline:_14&&_14[i]||null,shadow:_12&&_12[i]||null,cx:_11[i].x,cy:_11[i].y};
if(typeof run.data[0]=="number"){
o.x=i+1;
o.y=run.data[i];
}else{
o.x=run.data[i].x;
o.y=run.data[i].y;
}
this._connectEvents(o);
_1e[i]=o;
},this);
this._eventSeries[run.name]=_1e;
}else{
delete this._eventSeries[run.name];
}
run.dirty=false;
}
this.dirty=false;
if(_4("dojo-bidi")){
this._checkOrientation(this.group,_c,_d);
}
return this;
},_animateScatter:function(_1f,_20){
fx.animateTransform(_1.delegate({shape:_1f,duration:1200,transform:[{name:"translate",start:[0,_20],end:[0,0]},{name:"scale",start:[0,0],end:[1,1]},{name:"original"}]},this.animate)).play();
}});
});
