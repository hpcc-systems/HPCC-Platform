//>>built
define("dojox/charting/plot2d/CartesianBase",["dojo/_base/lang","dojo/_base/declare","dojo/_base/connect","dojo/has","./Base","../scaler/primitive","dojox/gfx","dojox/gfx/fx","dojox/lang/utils"],function(_1,_2,_3,_4,_5,_6,_7,fx,du){
return _2("dojox.charting.plot2d.CartesianBase",_5,{baseParams:{hAxis:"x",vAxis:"y",labels:false,labelOffset:10,fixed:true,precision:1,labelStyle:"inside",htmlLabels:true,omitLabels:true,labelFunc:null},constructor:function(_8,_9){
this.axes=["hAxis","vAxis"];
this.zoom=null;
this.zoomQueue=[];
this.lastWindow={vscale:1,hscale:1,xoffset:0,yoffset:0};
this.hAxis=(_9&&_9.hAxis)||"x";
this.vAxis=(_9&&_9.vAxis)||"y";
this.series=[];
this.opt=_1.clone(this.baseParams);
du.updateWithObject(this.opt,_9);
},clear:function(){
this.inherited(arguments);
this._hAxis=null;
this._vAxis=null;
return this;
},cleanGroup:function(_a,_b){
this.inherited(arguments);
if(!_b&&this.chart._nativeClip){
var _c=this.chart.offsets,_d=this.chart.dim;
var w=Math.max(0,_d.width-_c.l-_c.r),h=Math.max(0,_d.height-_c.t-_c.b);
this.group.setClip({x:_c.l,y:_c.t,width:w,height:h});
if(!this._clippedGroup){
this._clippedGroup=this.group.createGroup();
}
}
},purgeGroup:function(){
this.inherited(arguments);
this._clippedGroup=null;
},getGroup:function(){
return this._clippedGroup||this.group;
},setAxis:function(_e){
if(_e){
this[_e.vertical?"_vAxis":"_hAxis"]=_e;
}
return this;
},toPage:function(_f){
var ah=this._hAxis,av=this._vAxis,sh=ah.getScaler(),sv=av.getScaler(),th=sh.scaler.getTransformerFromModel(sh),tv=sv.scaler.getTransformerFromModel(sv),c=this.chart.getCoords(),o=this.chart.offsets,dim=this.chart.dim;
var t=function(_10){
var r={};
r.x=th(_10[ah.name])+c.x+o.l;
r.y=c.y+dim.height-o.b-tv(_10[av.name]);
return r;
};
return _f?t(_f):t;
},toData:function(_11){
var ah=this._hAxis,av=this._vAxis,sh=ah.getScaler(),sv=av.getScaler(),th=sh.scaler.getTransformerFromPlot(sh),tv=sv.scaler.getTransformerFromPlot(sv),c=this.chart.getCoords(),o=this.chart.offsets,dim=this.chart.dim;
var t=function(_12){
var r={};
r[ah.name]=th(_12.x-c.x-o.l);
r[av.name]=tv(c.y+dim.height-_12.y-o.b);
return r;
};
return _11?t(_11):t;
},isDirty:function(){
return this.dirty||this._hAxis&&this._hAxis.dirty||this._vAxis&&this._vAxis.dirty;
},createLabel:function(_13,_14,_15,_16){
if(this.opt.labels){
var x,y,_17=this.opt.labelFunc?this.opt.labelFunc.apply(this,[_14,this.opt.fixed,this.opt.precision]):this._getLabel(isNaN(_14.y)?_14:_14.y);
if(this.opt.labelStyle=="inside"){
var _18=_7._base._getTextBox(_17,{font:_16.series.font});
x=_15.x+_15.width/2;
y=_15.y+_15.height/2+_18.h/4;
if(_18.w>_15.width||_18.h>_15.height){
return;
}
}else{
x=_15.x+_15.width/2;
y=_15.y-this.opt.labelOffset;
}
this.renderLabel(_13,x,y,_17,_16,this.opt.labelStyle=="inside");
}
},performZoom:function(dim,_19){
var vs=this._vAxis.scale||1,hs=this._hAxis.scale||1,_1a=dim.height-_19.b,_1b=this._hScaler.bounds,_1c=(_1b.from-_1b.lower)*_1b.scale,_1d=this._vScaler.bounds,_1e=(_1d.from-_1d.lower)*_1d.scale,_1f=vs/this.lastWindow.vscale,_20=hs/this.lastWindow.hscale,_21=(this.lastWindow.xoffset-_1c)/((this.lastWindow.hscale==1)?hs:this.lastWindow.hscale),_22=(_1e-this.lastWindow.yoffset)/((this.lastWindow.vscale==1)?vs:this.lastWindow.vscale),_23=this.getGroup(),_24=fx.animateTransform(_1.delegate({shape:_23,duration:1200,transform:[{name:"translate",start:[0,0],end:[_19.l*(1-_20),_1a*(1-_1f)]},{name:"scale",start:[1,1],end:[_20,_1f]},{name:"original"},{name:"translate",start:[0,0],end:[_21,_22]}]},this.zoom));
_1.mixin(this.lastWindow,{vscale:vs,hscale:hs,xoffset:_1c,yoffset:_1e});
this.zoomQueue.push(_24);
_3.connect(_24,"onEnd",this,function(){
this.zoom=null;
this.zoomQueue.shift();
if(this.zoomQueue.length>0){
this.zoomQueue[0].play();
}
});
if(this.zoomQueue.length==1){
this.zoomQueue[0].play();
}
return this;
},initializeScalers:function(dim,_25){
if(this._hAxis){
if(!this._hAxis.initialized()){
this._hAxis.calculate(_25.hmin,_25.hmax,dim.width);
}
this._hScaler=this._hAxis.getScaler();
}else{
this._hScaler=_6.buildScaler(_25.hmin,_25.hmax,dim.width);
}
if(this._vAxis){
if(!this._vAxis.initialized()){
this._vAxis.calculate(_25.vmin,_25.vmax,dim.height);
}
this._vScaler=this._vAxis.getScaler();
}else{
this._vScaler=_6.buildScaler(_25.vmin,_25.vmax,dim.height);
}
return this;
}});
});
