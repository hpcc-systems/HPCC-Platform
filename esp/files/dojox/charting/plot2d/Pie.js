//>>built
define("dojox/charting/plot2d/Pie",["dojo/_base/lang","dojo/_base/array","dojo/_base/declare","./Base","./_PlotEvents","./common","dojox/gfx","dojox/gfx/matrix","dojox/lang/functional","dojox/lang/utils","dojo/has"],function(_1,_2,_3,_4,_5,dc,g,m,df,du,_6){
var _7=0.2;
return _3("dojox.charting.plot2d.Pie",[_4,_5],{defaultParams:{labels:true,ticks:false,fixed:true,precision:1,labelOffset:20,labelStyle:"default",htmlLabels:true,radGrad:"native",fanSize:5,startAngle:0},optionalParams:{radius:0,omitLabels:false,stroke:{},outline:{},shadow:{},fill:{},filter:{},styleFunc:null,font:"",fontColor:"",labelWiring:{}},constructor:function(_8,_9){
this.opt=_1.clone(this.defaultParams);
du.updateWithObject(this.opt,_9);
du.updateWithPattern(this.opt,_9,this.optionalParams);
this.axes=[];
this.run=null;
this.dyn=[];
},clear:function(){
this.inherited(arguments);
this.dyn=[];
this.run=null;
return this;
},setAxis:function(_a){
return this;
},addSeries:function(_b){
this.run=_b;
return this;
},getSeriesStats:function(){
return _1.delegate(dc.defaultStats);
},getRequiredColors:function(){
return this.run?this.run.data.length:0;
},render:function(_c,_d){
if(!this.dirty){
return this;
}
this.resetEvents();
this.dirty=false;
this._eventSeries={};
this.cleanGroup();
var s=this.group,t=this.chart.theme;
if(!this.run||!this.run.data.length){
return this;
}
var rx=(_c.width-_d.l-_d.r)/2,ry=(_c.height-_d.t-_d.b)/2,r=Math.min(rx,ry),_e="font" in this.opt?this.opt.font:t.series.font,_f,_10=m._degToRad(this.opt.startAngle),_11=_10,_12,_13,_14,_15,_16,run=this.run.data,_17=this.events();
this.dyn=[];
if("radius" in this.opt){
r=this.opt.radius;
_16=r-this.opt.labelOffset;
}
var _18={cx:_d.l+rx,cy:_d.t+ry,r:r};
if(this.opt.shadow||t.shadow){
var _19=this.opt.shadow||t.shadow;
var _1a=_1.clone(_18);
_1a.cx+=_19.dx;
_1a.cy+=_19.dy;
s.createCircle(_1a).setFill(_19.color).setStroke(_19);
}
if(s.setFilter&&(this.opt.filter||t.filter)){
s.createCircle(_18).setFill(t.series.stroke).setFilter(this.opt.filter||t.filter);
}
if(typeof run[0]=="number"){
_12=df.map(run,"x ? Math.max(x, 0) : 0");
if(df.every(_12,"<= 0")){
s.createCircle(_18).setStroke(t.series.stroke);
this.dyn=_2.map(_12,function(){
return {};
});
return this;
}else{
_13=df.map(_12,"/this",df.foldl(_12,"+",0));
if(this.opt.labels){
_14=_2.map(_13,function(x){
return x>0?this._getLabel(x*100)+"%":"";
},this);
}
}
}else{
_12=df.map(run,"x ? Math.max(x.y, 0) : 0");
if(df.every(_12,"<= 0")){
s.createCircle(_18).setStroke(t.series.stroke);
this.dyn=_2.map(_12,function(){
return {};
});
return this;
}else{
_13=df.map(_12,"/this",df.foldl(_12,"+",0));
if(this.opt.labels){
_14=_2.map(_13,function(x,i){
if(x<0){
return "";
}
var v=run[i];
return "text" in v?v.text:this._getLabel(x*100)+"%";
},this);
}
}
}
var _1b=df.map(run,function(v,i){
var _1c=[this.opt,this.run];
if(v!==null&&typeof v!="number"){
_1c.push(v);
}
if(this.opt.styleFunc){
_1c.push(this.opt.styleFunc(v));
}
return t.next("slice",_1c,true);
},this);
if(this.opt.labels){
_f=_e?g.normalizedLength(g.splitFontString(_e).size):0;
_15=df.foldl1(df.map(_14,function(_1d,i){
var _1e=_1b[i].series.font;
return g._base._getTextBox(_1d,{font:_1e}).w;
},this),"Math.max(a, b)")/2;
if(this.opt.labelOffset<0){
r=Math.min(rx-2*_15,ry-_f)+this.opt.labelOffset;
}
_16=r-this.opt.labelOffset;
}
var _1f=new Array(_13.length);
_2.some(_13,function(_20,i){
if(_20<0){
return false;
}
if(_20==0){
this.dyn.push({fill:null,stroke:null});
return false;
}
var v=run[i],_21=_1b[i],_22,o;
if(_20>=1){
_22=this._plotFill(_21.series.fill,_c,_d);
_22=this._shapeFill(_22,{x:_18.cx-_18.r,y:_18.cy-_18.r,width:2*_18.r,height:2*_18.r});
_22=this._pseudoRadialFill(_22,{x:_18.cx,y:_18.cy},_18.r);
var _23=s.createCircle(_18).setFill(_22).setStroke(_21.series.stroke);
this.dyn.push({fill:_22,stroke:_21.series.stroke});
if(_17){
o={element:"slice",index:i,run:this.run,shape:_23,x:i,y:typeof v=="number"?v:v.y,cx:_18.cx,cy:_18.cy,cr:r};
this._connectEvents(o);
_1f[i]=o;
}
return false;
}
var end=_11+_20*2*Math.PI;
if(i+1==_13.length){
end=_10+2*Math.PI;
}
var _24=end-_11,x1=_18.cx+r*Math.cos(_11),y1=_18.cy+r*Math.sin(_11),x2=_18.cx+r*Math.cos(end),y2=_18.cy+r*Math.sin(end);
var _25=m._degToRad(this.opt.fanSize);
if(_21.series.fill&&_21.series.fill.type==="radial"&&this.opt.radGrad==="fan"&&_24>_25){
var _26=s.createGroup(),_27=Math.ceil(_24/_25),_28=_24/_27;
_22=this._shapeFill(_21.series.fill,{x:_18.cx-_18.r,y:_18.cy-_18.r,width:2*_18.r,height:2*_18.r});
for(var j=0;j<_27;++j){
var _29=j==0?x1:_18.cx+r*Math.cos(_11+(j-_7)*_28),_2a=j==0?y1:_18.cy+r*Math.sin(_11+(j-_7)*_28),_2b=j==_27-1?x2:_18.cx+r*Math.cos(_11+(j+1+_7)*_28),_2c=j==_27-1?y2:_18.cy+r*Math.sin(_11+(j+1+_7)*_28);
_26.createPath().moveTo(_18.cx,_18.cy).lineTo(_29,_2a).arcTo(r,r,0,_28>Math.PI,true,_2b,_2c).lineTo(_18.cx,_18.cy).closePath().setFill(this._pseudoRadialFill(_22,{x:_18.cx,y:_18.cy},r,_11+(j+0.5)*_28,_11+(j+0.5)*_28));
}
_26.createPath().moveTo(_18.cx,_18.cy).lineTo(x1,y1).arcTo(r,r,0,_24>Math.PI,true,x2,y2).lineTo(_18.cx,_18.cy).closePath().setStroke(_21.series.stroke);
_23=_26;
}else{
_23=s.createPath().moveTo(_18.cx,_18.cy).lineTo(x1,y1).arcTo(r,r,0,_24>Math.PI,true,x2,y2).lineTo(_18.cx,_18.cy).closePath().setStroke(_21.series.stroke);
_22=_21.series.fill;
if(_22&&_22.type==="radial"){
_22=this._shapeFill(_22,{x:_18.cx-_18.r,y:_18.cy-_18.r,width:2*_18.r,height:2*_18.r});
if(this.opt.radGrad==="linear"){
_22=this._pseudoRadialFill(_22,{x:_18.cx,y:_18.cy},r,_11,end);
}
}else{
if(_22&&_22.type==="linear"){
_22=this._plotFill(_22,_c,_d);
_22=this._shapeFill(_22,_23.getBoundingBox());
}
}
_23.setFill(_22);
}
this.dyn.push({fill:_22,stroke:_21.series.stroke});
if(_17){
o={element:"slice",index:i,run:this.run,shape:_23,x:i,y:typeof v=="number"?v:v.y,cx:_18.cx,cy:_18.cy,cr:r};
this._connectEvents(o);
_1f[i]=o;
}
_11=end;
return false;
},this);
if(this.opt.labels){
var _2d=_6("dojo-bidi")&&this.chart.isRightToLeft();
if(this.opt.labelStyle=="default"){
_11=_10;
_2.some(_13,function(_2e,i){
if(_2e<=0){
return false;
}
var _2f=_1b[i];
if(_2e>=1){
this.renderLabel(s,_18.cx,_18.cy+_f/2,_14[i],_2f,this.opt.labelOffset>0);
return true;
}
var end=_11+_2e*2*Math.PI;
if(i+1==_13.length){
end=_10+2*Math.PI;
}
if(this.opt.omitLabels&&end-_11<0.001){
return false;
}
var _30=(_11+end)/2,x=_18.cx+_16*Math.cos(_30),y=_18.cy+_16*Math.sin(_30)+_f/2;
this.renderLabel(s,_2d?_c.width-x:x,y,_14[i],_2f,this.opt.labelOffset>0);
_11=end;
return false;
},this);
}else{
if(this.opt.labelStyle=="columns"){
_11=_10;
var _31=this.opt.omitLabels;
var _32=[];
_2.forEach(_13,function(_33,i){
var end=_11+_33*2*Math.PI;
if(i+1==_13.length){
end=_10+2*Math.PI;
}
var _34=(_11+end)/2;
_32.push({angle:_34,left:Math.cos(_34)<0,theme:_1b[i],index:i,omit:_31?end-_11<0.001:false});
_11=end;
});
var _35=g._base._getTextBox("a",{font:_e}).h;
this._getProperLabelRadius(_32,_35,_18.r*1.1);
_2.forEach(_32,function(_36,i){
if(!_36.omit){
var _37=_18.cx-_18.r*2,_38=_18.cx+_18.r*2,_39=g._base._getTextBox(_14[i],{font:_36.theme.series.font}).w,x=_18.cx+_36.labelR*Math.cos(_36.angle),y=_18.cy+_36.labelR*Math.sin(_36.angle),_3a=(_36.left)?(_37+_39):(_38-_39),_3b=(_36.left)?_37:_3a;
var _3c=s.createPath().moveTo(_18.cx+_18.r*Math.cos(_36.angle),_18.cy+_18.r*Math.sin(_36.angle));
if(Math.abs(_36.labelR*Math.cos(_36.angle))<_18.r*2-_39){
_3c.lineTo(x,y);
}
_3c.lineTo(_3a,y).setStroke(_36.theme.series.labelWiring);
this.renderLabel(s,_2d?_c.width-_39-_3b:_3b,y,_14[i],_36.theme,false,"left");
}
},this);
}
}
}
var esi=0;
this._eventSeries[this.run.name]=df.map(run,function(v){
return v<=0?null:_1f[esi++];
});
if(_6("dojo-bidi")){
this._checkOrientation(this.group,_c,_d);
}
return this;
},_getProperLabelRadius:function(_3d,_3e,_3f){
var _40,_41,_42=1,_43=1;
if(_3d.length==1){
_3d[0].labelR=_3f;
return;
}
for(var i=0;i<_3d.length;i++){
var _44=Math.abs(Math.sin(_3d[i].angle));
if(_3d[i].left){
if(_42>=_44){
_42=_44;
_40=_3d[i];
}
}else{
if(_43>=_44){
_43=_44;
_41=_3d[i];
}
}
}
_40.labelR=_41.labelR=_3f;
this._calculateLabelR(_40,_3d,_3e);
this._calculateLabelR(_41,_3d,_3e);
},_calculateLabelR:function(_45,_46,_47){
var i=_45.index,_48=_46.length,_49=_45.labelR,_4a;
while(!(_46[i%_48].left^_46[(i+1)%_48].left)){
if(!_46[(i+1)%_48].omit){
_4a=(Math.sin(_46[i%_48].angle)*_49+((_46[i%_48].left)?(-_47):_47))/Math.sin(_46[(i+1)%_48].angle);
_49=(_4a<_45.labelR)?_45.labelR:_4a;
_46[(i+1)%_48].labelR=_49;
}
i++;
}
i=_45.index;
var j=(i==0)?_48-1:i-1;
while(!(_46[i].left^_46[j].left)){
if(!_46[j].omit){
_4a=(Math.sin(_46[i].angle)*_49+((_46[i].left)?_47:(-_47)))/Math.sin(_46[j].angle);
_49=(_4a<_45.labelR)?_45.labelR:_4a;
_46[j].labelR=_49;
}
i--;
j--;
i=(i<0)?i+_46.length:i;
j=(j<0)?j+_46.length:j;
}
}});
});
