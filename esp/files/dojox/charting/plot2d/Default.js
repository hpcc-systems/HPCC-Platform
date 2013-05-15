//>>built
define("dojox/charting/plot2d/Default",["dojo/_base/lang","dojo/_base/declare","dojo/_base/array","dojo/has","./CartesianBase","./_PlotEvents","./common","dojox/lang/functional","dojox/lang/functional/reversed","dojox/lang/utils","dojox/gfx/fx"],function(_1,_2,_3,_4,_5,_6,dc,df,_7,du,fx){
var _8=_7.lambda("item.purgeGroup()");
var _9=1200;
return _2("dojox.charting.plot2d.Default",[_5,_6],{defaultParams:{lines:true,areas:false,markers:false,tension:"",animate:false,enableCache:false,interpolate:false},optionalParams:{stroke:{},outline:{},shadow:{},fill:{},filter:{},styleFunc:null,font:"",fontColor:"",marker:"",markerStroke:{},markerOutline:{},markerShadow:{},markerFill:{},markerFont:"",markerFontColor:""},constructor:function(_a,_b){
this.opt=_1.clone(_1.mixin(this.opt,this.defaultParams));
du.updateWithObject(this.opt,_b);
du.updateWithPattern(this.opt,_b,this.optionalParams);
this.animate=this.opt.animate;
},createPath:function(_c,_d,_e){
var _f;
if(this.opt.enableCache&&_c._pathFreePool.length>0){
_f=_c._pathFreePool.pop();
_f.setShape(_e);
_d.add(_f);
}else{
_f=_d.createPath(_e);
}
if(this.opt.enableCache){
_c._pathUsePool.push(_f);
}
return _f;
},buildSegments:function(i,_10){
var run=this.series[i],min=_10?Math.max(0,Math.floor(this._hScaler.bounds.from-1)):0,max=_10?Math.min(run.data.length,Math.ceil(this._hScaler.bounds.to)):run.data.length,_11=null,_12=[];
for(var j=min;j<max;j++){
if(run.data[j]!=null&&(_10||run.data[j].y!=null)){
if(!_11){
_11=[];
_12.push({index:j,rseg:_11});
}
_11.push((_10&&run.data[j].hasOwnProperty("y"))?run.data[j].y:run.data[j]);
}else{
if(!this.opt.interpolate||_10){
_11=null;
}
}
}
return _12;
},render:function(dim,_13){
if(this.zoom&&!this.isDataDirty()){
return this.performZoom(dim,_13);
}
this.resetEvents();
this.dirty=this.isDirty();
var s;
if(this.dirty){
_3.forEach(this.series,_8);
this._eventSeries={};
this.cleanGroup();
this.getGroup().setTransform(null);
s=this.getGroup();
df.forEachRev(this.series,function(_14){
_14.cleanGroup(s);
});
}
var t=this.chart.theme,_15,_16,_17=this.events();
for(var i=this.series.length-1;i>=0;--i){
var run=this.series[i];
if(!this.dirty&&!run.dirty){
t.skip();
this._reconnectEvents(run.name);
continue;
}
run.cleanGroup();
if(this.opt.enableCache){
run._pathFreePool=(run._pathFreePool?run._pathFreePool:[]).concat(run._pathUsePool?run._pathUsePool:[]);
run._pathUsePool=[];
}
if(!run.data.length){
run.dirty=false;
t.skip();
continue;
}
var _18=t.next(this.opt.areas?"area":"line",[this.opt,run],true),_19,ht=this._hScaler.scaler.getTransformerFromModel(this._hScaler),vt=this._vScaler.scaler.getTransformerFromModel(this._vScaler),_1a=this._eventSeries[run.name]=new Array(run.data.length);
s=run.group;
var _1b=_3.some(run.data,function(_1c){
return typeof _1c=="number"||(_1c&&!_1c.hasOwnProperty("x"));
});
var _1d=this.buildSegments(i,_1b);
for(var seg=0;seg<_1d.length;seg++){
var _1e=_1d[seg];
if(_1b){
_19=_3.map(_1e.rseg,function(v,i){
return {x:ht(i+_1e.index+1)+_13.l,y:dim.height-_13.b-vt(v),data:v};
},this);
}else{
_19=_3.map(_1e.rseg,function(v){
return {x:ht(v.x)+_13.l,y:dim.height-_13.b-vt(v.y),data:v};
},this);
}
if(_1b&&this.opt.interpolate){
while(seg<_1d.length){
seg++;
_1e=_1d[seg];
if(_1e){
_19=_19.concat(_3.map(_1e.rseg,function(v,i){
return {x:ht(i+_1e.index+1)+_13.l,y:dim.height-_13.b-vt(v),data:v};
},this));
}
}
}
var _1f=this.opt.tension?dc.curve(_19,this.opt.tension):"";
if(this.opt.areas&&_19.length>1){
var _20=this._plotFill(_18.series.fill,dim,_13),_21=_1.clone(_19);
if(this.opt.tension){
var _22="L"+_21[_21.length-1].x+","+(dim.height-_13.b)+" L"+_21[0].x+","+(dim.height-_13.b)+" L"+_21[0].x+","+_21[0].y;
run.dyn.fill=s.createPath(_1f+" "+_22).setFill(_20).getFill();
}else{
_21.push({x:_19[_19.length-1].x,y:dim.height-_13.b});
_21.push({x:_19[0].x,y:dim.height-_13.b});
_21.push(_19[0]);
run.dyn.fill=s.createPolyline(_21).setFill(_20).getFill();
}
}
if(this.opt.lines||this.opt.markers){
_15=_18.series.stroke;
if(_18.series.outline){
_16=run.dyn.outline=dc.makeStroke(_18.series.outline);
_16.width=2*_16.width+_15.width;
}
}
if(this.opt.markers){
run.dyn.marker=_18.symbol;
}
var _23=null,_24=null,_25=null;
if(_15&&_18.series.shadow&&_19.length>1){
var _26=_18.series.shadow,_27=_3.map(_19,function(c){
return {x:c.x+_26.dx,y:c.y+_26.dy};
});
if(this.opt.lines){
if(this.opt.tension){
run.dyn.shadow=s.createPath(dc.curve(_27,this.opt.tension)).setStroke(_26).getStroke();
}else{
run.dyn.shadow=s.createPolyline(_27).setStroke(_26).getStroke();
}
}
if(this.opt.markers&&_18.marker.shadow){
_26=_18.marker.shadow;
_25=_3.map(_27,function(c){
return this.createPath(run,s,"M"+c.x+" "+c.y+" "+_18.symbol).setStroke(_26).setFill(_26.color);
},this);
}
}
if(this.opt.lines&&_19.length>1){
var _28;
if(_16){
if(this.opt.tension){
run.dyn.outline=s.createPath(_1f).setStroke(_16).getStroke();
}else{
run.dyn.outline=s.createPolyline(_19).setStroke(_16).getStroke();
}
}
if(this.opt.tension){
run.dyn.stroke=(_28=s.createPath(_1f)).setStroke(_15).getStroke();
}else{
run.dyn.stroke=(_28=s.createPolyline(_19)).setStroke(_15).getStroke();
}
if(_28.setFilter&&_18.series.filter){
_28.setFilter(_18.series.filter);
}
}
var _29=null;
if(this.opt.markers){
var _2a=_18;
_23=new Array(_19.length);
_24=new Array(_19.length);
_16=null;
if(_2a.marker.outline){
_16=dc.makeStroke(_2a.marker.outline);
_16.width=2*_16.width+(_2a.marker.stroke?_2a.marker.stroke.width:0);
}
_3.forEach(_19,function(c,i){
if(this.opt.styleFunc||typeof c.data!="number"){
var _2b=typeof c.data!="number"?[c.data]:[];
if(this.opt.styleFunc){
_2b.push(this.opt.styleFunc(c.data));
}
_2a=t.addMixin(_18,"marker",_2b,true);
}else{
_2a=t.post(_18,"marker");
}
var _2c="M"+c.x+" "+c.y+" "+_2a.symbol;
if(_16){
_24[i]=this.createPath(run,s,_2c).setStroke(_16);
}
_23[i]=this.createPath(run,s,_2c).setStroke(_2a.marker.stroke).setFill(_2a.marker.fill);
},this);
run.dyn.markerFill=_2a.marker.fill;
run.dyn.markerStroke=_2a.marker.stroke;
if(!_29&&this.opt.labels){
_29=_23[0].getBoundingBox();
}
if(_17){
_3.forEach(_23,function(s,i){
var o={element:"marker",index:i+_1e.index,run:run,shape:s,outline:_24[i]||null,shadow:_25&&_25[i]||null,cx:_19[i].x,cy:_19[i].y};
if(_1b){
o.x=i+_1e.index+1;
o.y=run.data[i+_1e.index];
}else{
o.x=_1e.rseg[i].x;
o.y=run.data[i+_1e.index].y;
}
this._connectEvents(o);
_1a[i+_1e.index]=o;
},this);
}else{
delete this._eventSeries[run.name];
}
}
if(this.opt.labels){
var _2d=_29?_29.width:2;
var _2e=_29?_29.height:2;
_3.forEach(_19,function(c,i){
if(this.opt.styleFunc||typeof c.data!="number"){
var _2f=typeof c.data!="number"?[c.data]:[];
if(this.opt.styleFunc){
_2f.push(this.opt.styleFunc(c.data));
}
_2a=t.addMixin(_18,"marker",_2f,true);
}else{
_2a=t.post(_18,"marker");
}
this.createLabel(s,_1e.rseg[i],{x:c.x-_2d/2,y:c.y-_2e/2,width:_2d,height:_2e},_2a);
},this);
}
}
run.dirty=false;
}
if(_4("dojo-bidi")){
this._checkOrientation(this.group,dim,_13);
}
if(this.animate){
var _30=this.getGroup();
fx.animateTransform(_1.delegate({shape:_30,duration:_9,transform:[{name:"translate",start:[0,dim.height-_13.b],end:[0,0]},{name:"scale",start:[1,0],end:[1,1]},{name:"original"}]},this.animate)).play();
}
this.dirty=false;
return this;
}});
});
