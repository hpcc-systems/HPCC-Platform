//>>built
define("dojox/charting/Chart",["../main","dojo/_base/lang","dojo/_base/array","dojo/_base/declare","dojo/dom-style","dojo/dom","dojo/dom-geometry","dojo/dom-construct","dojo/_base/Color","dojo/sniff","./Element","./SimpleTheme","./Series","./axis2d/common","dojox/gfx/shape","dojox/gfx","dojo/has!dojo-bidi?./bidi/Chart","dojox/lang/functional","dojox/lang/functional/fold","dojox/lang/functional/reversed"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9,_a,_b,_c,_d,_e,_f,g,_10,_11){
var dc=_2.getObject("charting",true,_1),_12=_11.lambda("item.clear()"),_13=_11.lambda("item.purgeGroup()"),_14=_11.lambda("item.destroy()"),_15=_11.lambda("item.dirty = false"),_16=_11.lambda("item.dirty = true"),_17=_11.lambda("item.name");
var _18=_4(_a("dojo-bidi")?"dojox.charting.NonBidiChart":"dojox.charting.Chart",null,{constructor:function(_19,_1a){
if(!_1a){
_1a={};
}
this.margins=_1a.margins?_1a.margins:{l:10,t:10,r:10,b:10};
this.stroke=_1a.stroke;
this.fill=_1a.fill;
this.delayInMs=_1a.delayInMs||200;
this.title=_1a.title;
this.titleGap=_1a.titleGap;
this.titlePos=_1a.titlePos;
this.titleFont=_1a.titleFont;
this.titleFontColor=_1a.titleFontColor;
this.chartTitle=null;
this.htmlLabels=true;
if("htmlLabels" in _1a){
this.htmlLabels=_1a.htmlLabels;
}
this.theme=null;
this.axes={};
this.stack=[];
this.plots={};
this.series=[];
this.runs={};
this.dirty=true;
this.node=_6.byId(_19);
var box=_7.getMarginBox(_19);
this.surface=g.createSurface(this.node,box.w||400,box.h||300);
if(this.surface.declaredClass.indexOf("vml")==-1){
this._nativeClip=true;
}
},destroy:function(){
_3.forEach(this.series,_14);
_3.forEach(this.stack,_14);
_11.forIn(this.axes,_14);
this.surface.destroy();
if(this.chartTitle&&this.chartTitle.tagName){
_8.destroy(this.chartTitle);
}
},getCoords:function(){
var _1b=this.node;
var s=_5.getComputedStyle(_1b),_1c=_7.getMarginBox(_1b,s);
var abs=_7.position(_1b,true);
_1c.x=abs.x;
_1c.y=abs.y;
return _1c;
},setTheme:function(_1d){
this.theme=_1d.clone();
this.dirty=true;
return this;
},addAxis:function(_1e,_1f){
var _20,_21=_1f&&_1f.type||"Default";
if(typeof _21=="string"){
if(!dc.axis2d||!dc.axis2d[_21]){
throw Error("Can't find axis: "+_21+" - Check "+"require() dependencies.");
}
_20=new dc.axis2d[_21](this,_1f);
}else{
_20=new _21(this,_1f);
}
_20.name=_1e;
_20.dirty=true;
if(_1e in this.axes){
this.axes[_1e].destroy();
}
this.axes[_1e]=_20;
this.dirty=true;
return this;
},getAxis:function(_22){
return this.axes[_22];
},removeAxis:function(_23){
if(_23 in this.axes){
this.axes[_23].destroy();
delete this.axes[_23];
this.dirty=true;
}
return this;
},addPlot:function(_24,_25){
var _26,_27=_25&&_25.type||"Default";
if(typeof _27=="string"){
if(!dc.plot2d||!dc.plot2d[_27]){
throw Error("Can't find plot: "+_27+" - didn't you forget to dojo"+".require() it?");
}
_26=new dc.plot2d[_27](this,_25);
}else{
_26=new _27(this,_25);
}
_26.name=_24;
_26.dirty=true;
if(_24 in this.plots){
this.stack[this.plots[_24]].destroy();
this.stack[this.plots[_24]]=_26;
}else{
this.plots[_24]=this.stack.length;
this.stack.push(_26);
}
this.dirty=true;
return this;
},getPlot:function(_28){
return this.stack[this.plots[_28]];
},removePlot:function(_29){
if(_29 in this.plots){
var _2a=this.plots[_29];
delete this.plots[_29];
this.stack[_2a].destroy();
this.stack.splice(_2a,1);
_11.forIn(this.plots,function(idx,_2b,_2c){
if(idx>_2a){
_2c[_2b]=idx-1;
}
});
var ns=_3.filter(this.series,function(run){
return run.plot!=_29;
});
if(ns.length<this.series.length){
_3.forEach(this.series,function(run){
if(run.plot==_29){
run.destroy();
}
});
this.runs={};
_3.forEach(ns,function(run,_2d){
this.runs[run.plot]=_2d;
},this);
this.series=ns;
}
this.dirty=true;
}
return this;
},getPlotOrder:function(){
return _11.map(this.stack,_17);
},setPlotOrder:function(_2e){
var _2f={},_30=_11.filter(_2e,function(_31){
if(!(_31 in this.plots)||(_31 in _2f)){
return false;
}
_2f[_31]=1;
return true;
},this);
if(_30.length<this.stack.length){
_11.forEach(this.stack,function(_32){
var _33=_32.name;
if(!(_33 in _2f)){
_30.push(_33);
}
});
}
var _34=_11.map(_30,function(_35){
return this.stack[this.plots[_35]];
},this);
_11.forEach(_34,function(_36,i){
this.plots[_36.name]=i;
},this);
this.stack=_34;
this.dirty=true;
return this;
},movePlotToFront:function(_37){
if(_37 in this.plots){
var _38=this.plots[_37];
if(_38){
var _39=this.getPlotOrder();
_39.splice(_38,1);
_39.unshift(_37);
return this.setPlotOrder(_39);
}
}
return this;
},movePlotToBack:function(_3a){
if(_3a in this.plots){
var _3b=this.plots[_3a];
if(_3b<this.stack.length-1){
var _3c=this.getPlotOrder();
_3c.splice(_3b,1);
_3c.push(_3a);
return this.setPlotOrder(_3c);
}
}
return this;
},addSeries:function(_3d,_3e,_3f){
var run=new _d(this,_3e,_3f);
run.name=_3d;
if(_3d in this.runs){
this.series[this.runs[_3d]].destroy();
this.series[this.runs[_3d]]=run;
}else{
this.runs[_3d]=this.series.length;
this.series.push(run);
}
this.dirty=true;
if(!("ymin" in run)&&"min" in run){
run.ymin=run.min;
}
if(!("ymax" in run)&&"max" in run){
run.ymax=run.max;
}
return this;
},getSeries:function(_40){
return this.series[this.runs[_40]];
},removeSeries:function(_41){
if(_41 in this.runs){
var _42=this.runs[_41];
delete this.runs[_41];
this.series[_42].destroy();
this.series.splice(_42,1);
_11.forIn(this.runs,function(idx,_43,_44){
if(idx>_42){
_44[_43]=idx-1;
}
});
this.dirty=true;
}
return this;
},updateSeries:function(_45,_46,_47){
if(_45 in this.runs){
var run=this.series[this.runs[_45]];
run.update(_46);
if(_47){
this.dirty=true;
}else{
this._invalidateDependentPlots(run.plot,false);
this._invalidateDependentPlots(run.plot,true);
}
}
return this;
},getSeriesOrder:function(_48){
return _11.map(_11.filter(this.series,function(run){
return run.plot==_48;
}),_17);
},setSeriesOrder:function(_49){
var _4a,_4b={},_4c=_11.filter(_49,function(_4d){
if(!(_4d in this.runs)||(_4d in _4b)){
return false;
}
var run=this.series[this.runs[_4d]];
if(_4a){
if(run.plot!=_4a){
return false;
}
}else{
_4a=run.plot;
}
_4b[_4d]=1;
return true;
},this);
_11.forEach(this.series,function(run){
var _4e=run.name;
if(!(_4e in _4b)&&run.plot==_4a){
_4c.push(_4e);
}
});
var _4f=_11.map(_4c,function(_50){
return this.series[this.runs[_50]];
},this);
this.series=_4f.concat(_11.filter(this.series,function(run){
return run.plot!=_4a;
}));
_11.forEach(this.series,function(run,i){
this.runs[run.name]=i;
},this);
this.dirty=true;
return this;
},moveSeriesToFront:function(_51){
if(_51 in this.runs){
var _52=this.runs[_51],_53=this.getSeriesOrder(this.series[_52].plot);
if(_51!=_53[0]){
_53.splice(_52,1);
_53.unshift(_51);
return this.setSeriesOrder(_53);
}
}
return this;
},moveSeriesToBack:function(_54){
if(_54 in this.runs){
var _55=this.runs[_54],_56=this.getSeriesOrder(this.series[_55].plot);
if(_54!=_56[_56.length-1]){
_56.splice(_55,1);
_56.push(_54);
return this.setSeriesOrder(_56);
}
}
return this;
},resize:function(_57,_58){
switch(arguments.length){
case 1:
_7.setMarginBox(this.node,_57);
break;
case 2:
_7.setMarginBox(this.node,{w:_57,h:_58});
break;
}
var box=_7.getMarginBox(this.node);
var d=this.surface.getDimensions();
if(d.width!=box.w||d.height!=box.h){
this.surface.setDimensions(box.w,box.h);
this.dirty=true;
return this.render();
}else{
return this;
}
},getGeometry:function(){
var ret={};
_11.forIn(this.axes,function(_59){
if(_59.initialized()){
ret[_59.name]={name:_59.name,vertical:_59.vertical,scaler:_59.scaler,ticks:_59.ticks};
}
});
return ret;
},setAxisWindow:function(_5a,_5b,_5c,_5d){
var _5e=this.axes[_5a];
if(_5e){
_5e.setWindow(_5b,_5c);
_3.forEach(this.stack,function(_5f){
if(_5f.hAxis==_5a||_5f.vAxis==_5a){
_5f.zoom=_5d;
}
});
}
return this;
},setWindow:function(sx,sy,dx,dy,_60){
if(!("plotArea" in this)){
this.calculateGeometry();
}
_11.forIn(this.axes,function(_61){
var _62,_63,_64=_61.getScaler().bounds,s=_64.span/(_64.upper-_64.lower);
if(_61.vertical){
_62=sy;
_63=dy/s/_62;
}else{
_62=sx;
_63=dx/s/_62;
}
_61.setWindow(_62,_63);
});
_3.forEach(this.stack,function(_65){
_65.zoom=_60;
});
return this;
},zoomIn:function(_66,_67,_68){
var _69=this.axes[_66];
if(_69){
var _6a,_6b,_6c=_69.getScaler().bounds;
var _6d=Math.min(_67[0],_67[1]);
var _6e=Math.max(_67[0],_67[1]);
_6d=_67[0]<_6c.lower?_6c.lower:_6d;
_6e=_67[1]>_6c.upper?_6c.upper:_6e;
_6a=(_6c.upper-_6c.lower)/(_6e-_6d);
_6b=_6d-_6c.lower;
this.setAxisWindow(_66,_6a,_6b);
if(_68){
this.delayedRender();
}else{
this.render();
}
}
},calculateGeometry:function(){
if(this.dirty){
return this.fullGeometry();
}
var _6f=_3.filter(this.stack,function(_70){
return _70.dirty||(_70.hAxis&&this.axes[_70.hAxis].dirty)||(_70.vAxis&&this.axes[_70.vAxis].dirty);
},this);
_71(_6f,this.plotArea);
return this;
},fullGeometry:function(){
this._makeDirty();
_3.forEach(this.stack,_12);
if(!this.theme){
this.setTheme(new _c());
}
_3.forEach(this.series,function(run){
if(!(run.plot in this.plots)){
if(!dc.plot2d||!dc.plot2d.Default){
throw Error("Can't find plot: Default - didn't you forget to dojo"+".require() it?");
}
var _72=new dc.plot2d.Default(this,{});
_72.name=run.plot;
this.plots[run.plot]=this.stack.length;
this.stack.push(_72);
}
this.stack[this.plots[run.plot]].addSeries(run);
},this);
_3.forEach(this.stack,function(_73){
if(_73.assignAxes){
_73.assignAxes(this.axes);
}
},this);
var dim=this.dim=this.surface.getDimensions();
dim.width=g.normalizedLength(dim.width);
dim.height=g.normalizedLength(dim.height);
_11.forIn(this.axes,_12);
_71(this.stack,dim);
var _74=this.offsets={l:0,r:0,t:0,b:0};
var _75=this;
_11.forIn(this.axes,function(_76){
if(_a("dojo-bidi")){
_75._resetLeftBottom(_76);
}
_11.forIn(_76.getOffsets(),function(o,i){
_74[i]=Math.max(o,_74[i]);
});
});
if(this.title){
this.titleGap=(this.titleGap==0)?0:this.titleGap||this.theme.chart.titleGap||20;
this.titlePos=this.titlePos||this.theme.chart.titlePos||"top";
this.titleFont=this.titleFont||this.theme.chart.titleFont;
this.titleFontColor=this.titleFontColor||this.theme.chart.titleFontColor||"black";
var _77=g.normalizedLength(g.splitFontString(this.titleFont).size);
_74[this.titlePos=="top"?"t":"b"]+=(_77+this.titleGap);
}
_11.forIn(this.margins,function(o,i){
_74[i]+=o;
});
this.plotArea={width:dim.width-_74.l-_74.r,height:dim.height-_74.t-_74.b};
_11.forIn(this.axes,_12);
_71(this.stack,this.plotArea);
return this;
},render:function(){
if(this._delayedRenderHandle){
clearTimeout(this._delayedRenderHandle);
this._delayedRenderHandle=null;
}
if(this.theme){
this.theme.clear();
}
if(this.dirty){
return this.fullRender();
}
this.calculateGeometry();
_11.forEachRev(this.stack,function(_78){
_78.render(this.dim,this.offsets);
},this);
_11.forIn(this.axes,function(_79){
_79.render(this.dim,this.offsets);
},this);
this._makeClean();
return this;
},fullRender:function(){
this.fullGeometry();
var _7a=this.offsets,dim=this.dim;
var w=Math.max(0,dim.width-_7a.l-_7a.r),h=Math.max(0,dim.height-_7a.t-_7a.b);
_3.forEach(this.series,_13);
_11.forIn(this.axes,_13);
_3.forEach(this.stack,_13);
var _7b=this.surface.children;
if(_f.dispose){
for(var i=0;i<_7b.length;++i){
_f.dispose(_7b[i]);
}
}
if(this.chartTitle&&this.chartTitle.tagName){
_8.destroy(this.chartTitle);
}
this.surface.clear();
this.chartTitle=null;
this._renderChartBackground(dim,_7a);
if(this._nativeClip){
this._renderPlotBackground(dim,_7a,w,h);
}else{
this._renderPlotBackground(dim,_7a,w,h);
}
_11.foldr(this.stack,function(z,_7c){
return _7c.render(dim,_7a),0;
},0);
if(!this._nativeClip){
this._renderChartBackground(dim,_7a);
}
if(this.title){
var _7d=(g.renderer=="canvas")&&this.htmlLabels,_7e=_7d||!_a("ie")&&!_a("opera")&&this.htmlLabels?"html":"gfx",_7f=g.normalizedLength(g.splitFontString(this.titleFont).size);
this.chartTitle=_e.createText[_7e](this,this.surface,dim.width/2,this.titlePos=="top"?_7f+this.margins.t:dim.height-this.margins.b,"middle",this.title,this.titleFont,this.titleFontColor);
}
_11.forIn(this.axes,function(_80){
_80.render(dim,_7a);
});
this._makeClean();
return this;
},_renderChartBackground:function(dim,_81){
var t=this.theme,_82;
var _83=this.fill!==undefined?this.fill:(t.chart&&t.chart.fill);
var _84=this.stroke!==undefined?this.stroke:(t.chart&&t.chart.stroke);
if(_83=="inherit"){
var _85=this.node;
_83=new _9(_5.get(_85,"backgroundColor"));
while(_83.a==0&&_85!=document.documentElement){
_83=new _9(_5.get(_85,"backgroundColor"));
_85=_85.parentNode;
}
}
if(_83){
if(this._nativeClip){
_83=_b.prototype._shapeFill(_b.prototype._plotFill(_83,dim),{x:0,y:0,width:dim.width+1,height:dim.height+1});
this.surface.createRect({width:dim.width+1,height:dim.height+1}).setFill(_83);
}else{
_83=_b.prototype._plotFill(_83,dim,_81);
if(_81.l){
_82={x:0,y:0,width:_81.l,height:dim.height+1};
this.surface.createRect(_82).setFill(_b.prototype._shapeFill(_83,_82));
}
if(_81.r){
_82={x:dim.width-_81.r,y:0,width:_81.r+1,height:dim.height+2};
this.surface.createRect(_82).setFill(_b.prototype._shapeFill(_83,_82));
}
if(_81.t){
_82={x:0,y:0,width:dim.width+1,height:_81.t};
this.surface.createRect(_82).setFill(_b.prototype._shapeFill(_83,_82));
}
if(_81.b){
_82={x:0,y:dim.height-_81.b,width:dim.width+1,height:_81.b+2};
this.surface.createRect(_82).setFill(_b.prototype._shapeFill(_83,_82));
}
}
}
if(_84){
this.surface.createRect({width:dim.width-1,height:dim.height-1}).setStroke(_84);
}
},_renderPlotBackground:function(dim,_86,w,h){
var t=this.theme;
var _87=t.plotarea&&t.plotarea.fill;
var _88=t.plotarea&&t.plotarea.stroke;
var _89={x:_86.l-1,y:_86.t-1,width:w+2,height:h+2};
if(_87){
_87=_b.prototype._shapeFill(_b.prototype._plotFill(_87,dim,_86),_89);
this.surface.createRect(_89).setFill(_87);
}
if(_88){
this.surface.createRect({x:_86.l,y:_86.t,width:w+1,height:h+1}).setStroke(_88);
}
},delayedRender:function(){
if(!this._delayedRenderHandle){
this._delayedRenderHandle=setTimeout(_2.hitch(this,function(){
this.render();
}),this.delayInMs);
}
return this;
},connectToPlot:function(_8a,_8b,_8c){
return _8a in this.plots?this.stack[this.plots[_8a]].connect(_8b,_8c):null;
},fireEvent:function(_8d,_8e,_8f){
if(_8d in this.runs){
var _90=this.series[this.runs[_8d]].plot;
if(_90 in this.plots){
var _91=this.stack[this.plots[_90]];
if(_91){
_91.fireEvent(_8d,_8e,_8f);
}
}
}
return this;
},_makeClean:function(){
_3.forEach(this.axes,_15);
_3.forEach(this.stack,_15);
_3.forEach(this.series,_15);
this.dirty=false;
},_makeDirty:function(){
_3.forEach(this.axes,_16);
_3.forEach(this.stack,_16);
_3.forEach(this.series,_16);
this.dirty=true;
},_invalidateDependentPlots:function(_92,_93){
if(_92 in this.plots){
var _94=this.stack[this.plots[_92]],_95,_96=_93?"vAxis":"hAxis";
if(_94[_96]){
_95=this.axes[_94[_96]];
if(_95&&_95.dependOnData()){
_95.dirty=true;
_3.forEach(this.stack,function(p){
if(p[_96]&&p[_96]==_94[_96]){
p.dirty=true;
}
});
}
}else{
_94.dirty=true;
}
}
},setDir:function(dir){
return this;
},_resetLeftBottom:function(_97){
},formatTruncatedLabel:function(_98,_99,_9a){
}});
function _9b(_9c){
return {min:_9c.hmin,max:_9c.hmax};
};
function _9d(_9e){
return {min:_9e.vmin,max:_9e.vmax};
};
function _9f(_a0,h){
_a0.hmin=h.min;
_a0.hmax=h.max;
};
function _a1(_a2,v){
_a2.vmin=v.min;
_a2.vmax=v.max;
};
function _a3(_a4,_a5){
if(_a4&&_a5){
_a4.min=Math.min(_a4.min,_a5.min);
_a4.max=Math.max(_a4.max,_a5.max);
}
return _a4||_a5;
};
function _71(_a6,_a7){
var _a8={},_a9={};
_3.forEach(_a6,function(_aa){
var _ab=_a8[_aa.name]=_aa.getSeriesStats();
if(_aa.hAxis){
_a9[_aa.hAxis]=_a3(_a9[_aa.hAxis],_9b(_ab));
}
if(_aa.vAxis){
_a9[_aa.vAxis]=_a3(_a9[_aa.vAxis],_9d(_ab));
}
});
_3.forEach(_a6,function(_ac){
var _ad=_a8[_ac.name];
if(_ac.hAxis){
_9f(_ad,_a9[_ac.hAxis]);
}
if(_ac.vAxis){
_a1(_ad,_a9[_ac.vAxis]);
}
_ac.initializeScalers(_a7,_ad);
});
};
return _a("dojo-bidi")?_4("dojox.charting.Chart",[_18,_10]):_18;
});
