//>>built
define("dojox/charting/plot2d/Spider",["dojo/_base/lang","dojo/_base/declare","dojo/_base/connect","dojo/_base/array","dojo/dom-geometry","dojo/_base/fx","dojo/fx","dojo/sniff","./Base","./_PlotEvents","./common","../axis2d/common","dojox/gfx","dojox/gfx/matrix","dojox/gfx/fx","dojox/lang/functional","dojox/lang/utils","dojo/fx/easing"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9,_a,dc,da,g,m,_b,df,du,_c){
var _d=0.2;
var _e=_2("dojox.charting.plot2d.Spider",[_9,_a],{defaultParams:{labels:true,ticks:false,fixed:true,precision:1,labelOffset:-10,labelStyle:"default",htmlLabels:true,startAngle:-90,divisions:3,axisColor:"",axisWidth:0,spiderColor:"",spiderWidth:0,seriesWidth:0,seriesFillAlpha:0.2,spiderOrigin:0.16,markerSize:3,spiderType:"polygon",animationType:_c.backOut,axisTickFont:"",axisTickFontColor:"",axisFont:"",axisFontColor:""},optionalParams:{radius:0,font:"",fontColor:""},constructor:function(_f,_10){
this.opt=_1.clone(this.defaultParams);
du.updateWithObject(this.opt,_10);
du.updateWithPattern(this.opt,_10,this.optionalParams);
this.dyn=[];
this.datas={};
this.labelKey=[];
this.oldSeriePoints={};
this.animations={};
},clear:function(){
this.inherited(arguments);
this.dyn=[];
this.axes=[];
this.datas={};
this.labelKey=[];
this.oldSeriePoints={};
this.animations={};
return this;
},setAxis:function(_11){
if(_11){
if(_11.opt.min!=undefined){
this.datas[_11.name].min=_11.opt.min;
}
if(_11.opt.max!=undefined){
this.datas[_11.name].max=_11.opt.max;
}
}
return this;
},addSeries:function(run){
this.series.push(run);
var key;
for(key in run.data){
var val=run.data[key],_12=this.datas[key];
if(_12){
_12.vlist.push(val);
_12.min=Math.min(_12.min,val);
_12.max=Math.max(_12.max,val);
}else{
var _13="__"+key;
this.axes.push(_13);
this[_13]=key;
this.datas[key]={min:val,max:val,vlist:[val]};
}
}
if(this.labelKey.length<=0){
for(key in run.data){
this.labelKey.push(key);
}
}
return this;
},getSeriesStats:function(){
return dc.collectSimpleStats(this.series);
},render:function(dim,_14){
if(!this.dirty){
return this;
}
this.dirty=false;
this.cleanGroup();
var s=this.group,t=this.chart.theme;
this.resetEvents();
if(!this.series||!this.series.length){
return this;
}
var o=this.opt,ta=t.axis,rx=(dim.width-_14.l-_14.r)/2,ry=(dim.height-_14.t-_14.b)/2,r=Math.min(rx,ry),_15=o.font||(ta.majorTick&&ta.majorTick.font)||(ta.tick&&ta.tick.font)||"normal normal normal 7pt Tahoma",_16=o.axisFont||(ta.tick&&ta.tick.titleFont)||"normal normal normal 11pt Tahoma",_17=o.axisTickFontColor||(ta.majorTick&&ta.majorTick.fontColor)||(ta.tick&&ta.tick.fontColor)||"silver",_18=o.axisFontColor||(ta.tick&&ta.tick.titleFontColor)||"black",_19=o.axisColor||(ta.tick&&ta.tick.axisColor)||"silver",_1a=o.spiderColor||(ta.tick&&ta.tick.spiderColor)||"silver",_1b=o.axisWidth||(ta.stroke&&ta.stroke.width)||2,_1c=o.spiderWidth||(ta.stroke&&ta.stroke.width)||2,_1d=o.seriesWidth||(ta.stroke&&ta.stroke.width)||2,_1e=g.normalizedLength(g.splitFontString(_16).size),_1f=m._degToRad(o.startAngle),_20=_1f,_21,_22,_23,_24,_25,_26,_27,_28,ro=o.spiderOrigin,dv=o.divisions>=3?o.divisions:3,ms=o.markerSize,spt=o.spiderType,at=o.animationType,_29=o.labelOffset<-10?o.labelOffset:-10,_2a=0.2,i,j,_2b,len,_2c,_2d,_2e,run,_2f,min,max,_30;
if(o.labels){
_21=_4.map(this.series,function(s){
return s.name;
},this);
_22=df.foldl1(df.map(_21,function(_31){
var _32=t.series.font;
return g._base._getTextBox(_31,{font:_32}).w;
},this),"Math.max(a, b)")/2;
r=Math.min(rx-2*_22,ry-_1e)+_29;
_23=r-_29;
}
if("radius" in o){
r=o.radius;
_23=r-_29;
}
r/=(1+_2a);
var _33={cx:_14.l+rx,cy:_14.t+ry,r:r};
for(i=this.series.length-1;i>=0;i--){
_2e=this.series[i];
if(!this.dirty&&!_2e.dirty){
t.skip();
continue;
}
_2e.cleanGroup();
run=_2e.data;
if(run!==null){
len=this._getObjectLength(run);
if(!_24||_24.length<=0){
_24=[],_25=[],_28=[];
this._buildPoints(_24,len,_33,r,_20,true,dim);
this._buildPoints(_25,len,_33,r*ro,_20,true,dim);
this._buildPoints(_28,len,_33,_23,_20,false,dim);
if(dv>2){
_26=[],_27=[];
for(j=0;j<dv-2;j++){
_26[j]=[];
this._buildPoints(_26[j],len,_33,r*(ro+(1-ro)*(j+1)/(dv-1)),_20,true,dim);
_27[j]=r*(ro+(1-ro)*(j+1)/(dv-1));
}
}
}
}
}
var _34=s.createGroup(),_35={color:_19,width:_1b},_36={color:_1a,width:_1c};
for(j=_24.length-1;j>=0;--j){
_2b=_24[j];
var st={x:_2b.x+(_2b.x-_33.cx)*_2a,y:_2b.y+(_2b.y-_33.cy)*_2a},nd={x:_2b.x+(_2b.x-_33.cx)*_2a/2,y:_2b.y+(_2b.y-_33.cy)*_2a/2};
_34.createLine({x1:_33.cx,y1:_33.cy,x2:st.x,y2:st.y}).setStroke(_35);
this._drawArrow(_34,st,nd,_35);
}
var _37=s.createGroup();
for(j=_28.length-1;j>=0;--j){
_2b=_28[j];
_2c=g._base._getTextBox(this.labelKey[j],{font:_16}).w||0;
_2d=this.opt.htmlLabels&&g.renderer!="vml"?"html":"gfx";
var _38=da.createText[_2d](this.chart,_37,(!_5.isBodyLtr()&&_2d=="html")?(_2b.x+_2c-dim.width):_2b.x,_2b.y,"middle",this.labelKey[j],_16,_18);
if(this.opt.htmlLabels){
this.htmlElements.push(_38);
}
}
var _39=s.createGroup();
if(spt=="polygon"){
_39.createPolyline(_24).setStroke(_36);
_39.createPolyline(_25).setStroke(_36);
if(_26.length>0){
for(j=_26.length-1;j>=0;--j){
_39.createPolyline(_26[j]).setStroke(_36);
}
}
}else{
_39.createCircle({cx:_33.cx,cy:_33.cy,r:r}).setStroke(_36);
_39.createCircle({cx:_33.cx,cy:_33.cy,r:r*ro}).setStroke(_36);
if(_27.length>0){
for(j=_27.length-1;j>=0;--j){
_39.createCircle({cx:_33.cx,cy:_33.cy,r:_27[j]}).setStroke(_36);
}
}
}
len=this._getObjectLength(this.datas);
var _3a=s.createGroup(),k=0;
for(var key in this.datas){
_2f=this.datas[key];
min=_2f.min;
max=_2f.max;
_30=max-min;
end=_20+2*Math.PI*k/len;
for(i=0;i<dv;i++){
var _3b=min+_30*i/(dv-1);
_2b=this._getCoordinate(_33,r*(ro+(1-ro)*i/(dv-1)),end,dim);
_3b=this._getLabel(_3b);
_2c=g._base._getTextBox(_3b,{font:_15}).w||0;
_2d=this.opt.htmlLabels&&g.renderer!="vml"?"html":"gfx";
if(this.opt.htmlLabels){
this.htmlElements.push(da.createText[_2d](this.chart,_3a,(!_5.isBodyLtr()&&_2d=="html")?(_2b.x+_2c-dim.width):_2b.x,_2b.y,"start",_3b,_15,_17));
}
}
k++;
}
this.chart.seriesShapes={};
for(i=this.series.length-1;i>=0;i--){
_2e=this.series[i];
run=_2e.data;
if(run!==null){
var _3c=[],_3d=[];
k=0;
for(key in run){
_2f=this.datas[key];
min=_2f.min;
max=_2f.max;
_30=max-min;
var _3e=run[key],end=_20+2*Math.PI*k/len;
_2b=this._getCoordinate(_33,r*(ro+(1-ro)*(_3e-min)/_30),end,dim);
_3c.push(_2b);
_3d.push({sname:_2e.name,key:key,data:_3e});
k++;
}
_3c[_3c.length]=_3c[0];
_3d[_3d.length]=_3d[0];
var _3f=this._getBoundary(_3c),_40=t.next("spider",[o,_2e]),ts=_2e.group,f=g.normalizeColor(_40.series.fill),sk={color:_40.series.fill,width:_1d};
f.a=o.seriesFillAlpha;
_2e.dyn={fill:f,stroke:sk};
var _41=this.oldSeriePoints[_2e.name];
var cs=this._createSeriesEntry(ts,(_41||_25),_3c,f,sk,r,ro,ms,at);
this.chart.seriesShapes[_2e.name]=cs;
this.oldSeriePoints[_2e.name]=_3c;
var po={element:"spider_poly",index:i,id:"spider_poly_"+_2e.name,run:_2e,plot:this,shape:cs.poly,parent:ts,brect:_3f,cx:_33.cx,cy:_33.cy,cr:r,f:f,s:s};
this._connectEvents(po);
var so={element:"spider_plot",index:i,id:"spider_plot_"+_2e.name,run:_2e,plot:this,shape:_2e.group};
this._connectEvents(so);
_4.forEach(cs.circles,function(c,i){
var co={element:"spider_circle",index:i,id:"spider_circle_"+_2e.name+i,run:_2e,plot:this,shape:c,parent:ts,tdata:_3d[i],cx:_3c[i].x,cy:_3c[i].y,f:f,s:s};
this._connectEvents(co);
},this);
}
}
return this;
},_createSeriesEntry:function(ts,_42,sps,f,sk,r,ro,ms,at){
var _43=ts.createPolyline(_42).setFill(f).setStroke(sk),_44=[];
for(var j=0;j<_42.length;j++){
var _45=_42[j],cr=ms;
var _46=ts.createCircle({cx:_45.x,cy:_45.y,r:cr}).setFill(f).setStroke(sk);
_44.push(_46);
}
var _47=_4.map(sps,function(np,j){
var sp=_42[j],_48=new _6.Animation({duration:1000,easing:at,curve:[sp.y,np.y]});
var spl=_43,sc=_44[j];
_3.connect(_48,"onAnimate",function(y){
var _49=spl.getShape();
_49.points[j].y=y;
spl.setShape(_49);
var _4a=sc.getShape();
_4a.cy=y;
sc.setShape(_4a);
});
return _48;
});
var _4b=_4.map(sps,function(np,j){
var sp=_42[j],_4c=new _6.Animation({duration:1000,easing:at,curve:[sp.x,np.x]});
var spl=_43,sc=_44[j];
_3.connect(_4c,"onAnimate",function(x){
var _4d=spl.getShape();
_4d.points[j].x=x;
spl.setShape(_4d);
var _4e=sc.getShape();
_4e.cx=x;
sc.setShape(_4e);
});
return _4c;
});
var _4f=_7.combine(_47.concat(_4b));
_4f.play();
return {group:ts,poly:_43,circles:_44};
},plotEvent:function(o){
if(o.element=="spider_plot"){
if(o.type=="onmouseover"&&!_8("ie")){
o.shape.moveToFront();
}
}
},tooltipFunc:function(o){
if(o.element=="spider_circle"){
return o.tdata.sname+"<br/>"+o.tdata.key+"<br/>"+o.tdata.data;
}else{
return null;
}
},_getBoundary:function(_50){
var _51=_50[0].x,_52=_50[0].x,_53=_50[0].y,_54=_50[0].y;
for(var i=0;i<_50.length;i++){
var _55=_50[i];
_51=Math.max(_55.x,_51);
_53=Math.max(_55.y,_53);
_52=Math.min(_55.x,_52);
_54=Math.min(_55.y,_54);
}
return {x:_52,y:_54,width:_51-_52,height:_53-_54};
},_drawArrow:function(s,_56,end,_57){
var len=Math.sqrt(Math.pow(end.x-_56.x,2)+Math.pow(end.y-_56.y,2)),sin=(end.y-_56.y)/len,cos=(end.x-_56.x)/len,_58={x:end.x+(len/3)*(-sin),y:end.y+(len/3)*cos},_59={x:end.x+(len/3)*sin,y:end.y+(len/3)*(-cos)};
s.createPolyline([_56,_58,_59]).setFill(_57.color).setStroke(_57);
},_buildPoints:function(_5a,_5b,_5c,_5d,_5e,_5f,dim){
for(var i=0;i<_5b;i++){
var end=_5e+2*Math.PI*i/_5b;
_5a.push(this._getCoordinate(_5c,_5d,end,dim));
}
if(_5f){
_5a.push(this._getCoordinate(_5c,_5d,_5e+2*Math.PI,dim));
}
},_getCoordinate:function(_60,_61,_62,dim){
var x=_60.cx+_61*Math.cos(_62);
if(_8("dojo-bidi")&&this.chart.isRightToLeft()&&dim){
x=dim.width-x;
}
return {x:x,y:_60.cy+_61*Math.sin(_62)};
},_getObjectLength:function(obj){
var _63=0;
if(_1.isObject(obj)){
for(var key in obj){
_63++;
}
}
return _63;
},_getLabel:function(_64){
return dc.getLabel(_64,this.opt.fixed,this.opt.precision);
}});
return _e;
});
