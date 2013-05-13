//>>built
define("dojox/charting/axis2d/Default",["dojo/_base/lang","dojo/_base/array","dojo/sniff","dojo/_base/declare","dojo/_base/connect","dojo/dom-geometry","./Invisible","../scaler/linear","./common","dojox/gfx","dojox/lang/utils","dojox/lang/functional","dojo/has!dojo-bidi?../bidi/axis2d/Default"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9,g,du,df,_a){
var _b=45;
var _c=_4(_3("dojo-bidi")?"dojox.charting.axis2d.NonBidiDefault":"dojox.charting.axis2d.Default",_7,{defaultParams:{vertical:false,fixUpper:"none",fixLower:"none",natural:false,leftBottom:true,includeZero:false,fixed:true,majorLabels:true,minorTicks:true,minorLabels:true,microTicks:false,rotation:0,htmlLabels:true,enableCache:false,dropLabels:true,labelSizeChange:false},optionalParams:{min:0,max:1,from:0,to:1,majorTickStep:4,minorTickStep:2,microTickStep:1,labels:[],labelFunc:null,maxLabelSize:0,maxLabelCharCount:0,trailingSymbol:null,stroke:{},majorTick:{},minorTick:{},microTick:{},tick:{},font:"",fontColor:"",title:"",titleGap:0,titleFont:"",titleFontColor:"",titleOrientation:""},constructor:function(_d,_e){
this.opt=_1.clone(this.defaultParams);
du.updateWithObject(this.opt,_e);
du.updateWithPattern(this.opt,_e,this.optionalParams);
if(this.opt.enableCache){
this._textFreePool=[];
this._lineFreePool=[];
this._textUsePool=[];
this._lineUsePool=[];
}
this._invalidMaxLabelSize=true;
},setWindow:function(_f,_10){
if(_f!=this.scale){
this._invalidMaxLabelSize=true;
}
return this.inherited(arguments);
},_groupLabelWidth:function(_11,_12,_13){
if(!_11.length){
return 0;
}
if(_11.length>50){
_11.length=50;
}
if(_1.isObject(_11[0])){
_11=df.map(_11,function(_14){
return _14.text;
});
}
if(_13){
_11=df.map(_11,function(_15){
return _1.trim(_15).length==0?"":_15.substring(0,_13)+this.trailingSymbol;
},this);
}
var s=_11.join("<br>");
return g._base._getTextBox(s,{font:_12}).w||0;
},_getMaxLabelSize:function(min,max,_16,_17,_18,_19){
if(this._maxLabelSize==null&&arguments.length==6){
var o=this.opt;
this.scaler.minMinorStep=this._prevMinMinorStep=0;
var ob=_1.clone(o);
delete ob.to;
delete ob.from;
var sb=_8.buildScaler(min,max,_16,ob,o.to-o.from);
sb.minMinorStep=0;
this._majorStart=sb.major.start;
var tb=_8.buildTicks(sb,o);
if(_19&&tb){
var _1a=0,_1b=0;
var _1c=function(_1d){
if(_1d.label){
this.push(_1d.label);
}
};
var _1e=[];
if(this.opt.majorLabels){
_2.forEach(tb.major,_1c,_1e);
_1a=this._groupLabelWidth(_1e,_18,ob.maxLabelCharCount);
if(ob.maxLabelSize){
_1a=Math.min(ob.maxLabelSize,_1a);
}
}
_1e=[];
if(this.opt.dropLabels&&this.opt.minorLabels){
_2.forEach(tb.minor,_1c,_1e);
_1b=this._groupLabelWidth(_1e,_18,ob.maxLabelCharCount);
if(ob.maxLabelSize){
_1b=Math.min(ob.maxLabelSize,_1b);
}
}
this._maxLabelSize={majLabelW:_1a,minLabelW:_1b,majLabelH:_19,minLabelH:_19};
}else{
this._maxLabelSize=null;
}
}
return this._maxLabelSize;
},calculate:function(min,max,_1f){
this.inherited(arguments);
this.scaler.minMinorStep=this._prevMinMinorStep;
if((this._invalidMaxLabelSize||_1f!=this._oldSpan)&&(min!=Infinity&&max!=-Infinity)){
this._invalidMaxLabelSize=false;
if(this.opt.labelSizeChange){
this._maxLabelSize=null;
}
this._oldSpan=_1f;
var o=this.opt;
var ta=this.chart.theme.axis,_20=o.rotation%360,_21=this.chart.theme.axis.tick.labelGap,_22=o.font||(ta.majorTick&&ta.majorTick.font)||(ta.tick&&ta.tick.font),_23=_22?g.normalizedLength(g.splitFontString(_22).size):0,_24=this._getMaxLabelSize(min,max,_1f,_20,_22,_23);
if(typeof _21!="number"){
_21=4;
}
if(_24&&o.dropLabels){
var _25=Math.abs(Math.cos(_20*Math.PI/180)),_26=Math.abs(Math.sin(_20*Math.PI/180));
var _27,_28;
if(_20<0){
_20+=360;
}
switch(_20){
case 0:
case 180:
if(this.vertical){
_27=_28=_23;
}else{
_27=_24.majLabelW;
_28=_24.minLabelW;
}
break;
case 90:
case 270:
if(this.vertical){
_27=_24.majLabelW;
_28=_24.minLabelW;
}else{
_27=_28=_23;
}
break;
default:
_27=this.vertical?Math.min(_24.majLabelW,_23/_25):Math.min(_24.majLabelW,_23/_26);
var _29=Math.sqrt(_24.minLabelW*_24.minLabelW+_23*_23),_2a=this.vertical?_23*_25+_24.minLabelW*_26:_24.minLabelW*_25+_23*_26;
_28=Math.min(_29,_2a);
break;
}
this.scaler.minMinorStep=this._prevMinMinorStep=Math.max(_27,_28)+_21;
var _2b=this.scaler.minMinorStep<=this.scaler.minor.tick*this.scaler.bounds.scale;
if(!_2b){
this._skipInterval=Math.floor((_27+_21)/(this.scaler.major.tick*this.scaler.bounds.scale));
}else{
this._skipInterval=0;
}
}else{
this._skipInterval=0;
}
}
this.ticks=_8.buildTicks(this.scaler,this.opt);
return this;
},getOffsets:function(){
var s=this.scaler,_2c={l:0,r:0,t:0,b:0};
if(!s){
return _2c;
}
var o=this.opt,ta=this.chart.theme.axis,_2d=this.chart.theme.axis.tick.labelGap,_2e=o.titleFont||(ta.title&&ta.title.font),_2f=(o.titleGap==0)?0:o.titleGap||(ta.title&&ta.title.gap),_30=this.chart.theme.getTick("major",o),_31=this.chart.theme.getTick("minor",o),_32=_2e?g.normalizedLength(g.splitFontString(_2e).size):0,_33=o.rotation%360,_34=o.leftBottom,_35=Math.abs(Math.cos(_33*Math.PI/180)),_36=Math.abs(Math.sin(_33*Math.PI/180));
this.trailingSymbol=(o.trailingSymbol===undefined||o.trailingSymbol===null)?this.trailingSymbol:o.trailingSymbol;
if(typeof _2d!="number"){
_2d=4;
}
if(_33<0){
_33+=360;
}
var _37=this._getMaxLabelSize();
if(_37){
var _38;
var _39=Math.ceil(Math.max(_37.majLabelW,_37.minLabelW))+1,_3a=Math.ceil(Math.max(_37.majLabelH,_37.minLabelH))+1;
if(this.vertical){
_38=_34?"l":"r";
switch(_33){
case 0:
case 180:
_2c[_38]=_39;
_2c.t=_2c.b=_3a/2;
break;
case 90:
case 270:
_2c[_38]=_3a;
_2c.t=_2c.b=_39/2;
break;
default:
if(_33<=_b||(180<_33&&_33<=(180+_b))){
_2c[_38]=_3a*_36/2+_39*_35;
_2c[_34?"t":"b"]=_3a*_35/2+_39*_36;
_2c[_34?"b":"t"]=_3a*_35/2;
}else{
if(_33>(360-_b)||(180>_33&&_33>(180-_b))){
_2c[_38]=_3a*_36/2+_39*_35;
_2c[_34?"b":"t"]=_3a*_35/2+_39*_36;
_2c[_34?"t":"b"]=_3a*_35/2;
}else{
if(_33<90||(180<_33&&_33<270)){
_2c[_38]=_3a*_36+_39*_35;
_2c[_34?"t":"b"]=_3a*_35+_39*_36;
}else{
_2c[_38]=_3a*_36+_39*_35;
_2c[_34?"b":"t"]=_3a*_35+_39*_36;
}
}
}
break;
}
_2c[_38]+=_2d+Math.max(_30.length>0?_30.length:0,_31.length>0?_31.length:0)+(o.title?(_32+_2f):0);
}else{
_38=_34?"b":"t";
switch(_33){
case 0:
case 180:
_2c[_38]=_3a;
_2c.l=_2c.r=_39/2;
break;
case 90:
case 270:
_2c[_38]=_39;
_2c.l=_2c.r=_3a/2;
break;
default:
if((90-_b)<=_33&&_33<=90||(270-_b)<=_33&&_33<=270){
_2c[_38]=_3a*_35/2+_39*_36;
_2c[_34?"r":"l"]=_3a*_36/2+_39*_35;
_2c[_34?"l":"r"]=_3a*_36/2;
}else{
if(90<=_33&&_33<=(90+_b)||270<=_33&&_33<=(270+_b)){
_2c[_38]=_3a*_35/2+_39*_36;
_2c[_34?"l":"r"]=_3a*_36/2+_39*_35;
_2c[_34?"r":"l"]=_3a*_36/2;
}else{
if(_33<_b||(180<_33&&_33<(180+_b))){
_2c[_38]=_3a*_35+_39*_36;
_2c[_34?"r":"l"]=_3a*_36+_39*_35;
}else{
_2c[_38]=_3a*_35+_39*_36;
_2c[_34?"l":"r"]=_3a*_36+_39*_35;
}
}
}
break;
}
_2c[_38]+=_2d+Math.max(_30.length>0?_30.length:0,_31.length>0?_31.length:0)+(o.title?(_32+_2f):0);
}
}
return _2c;
},cleanGroup:function(_3b){
if(this.opt.enableCache&&this.group){
this._lineFreePool=this._lineFreePool.concat(this._lineUsePool);
this._lineUsePool=[];
this._textFreePool=this._textFreePool.concat(this._textUsePool);
this._textUsePool=[];
}
this.inherited(arguments);
},createText:function(_3c,_3d,x,y,_3e,_3f,_40,_41,_42){
if(!this.opt.enableCache||_3c=="html"){
return _9.createText[_3c](this.chart,_3d,x,y,_3e,_3f,_40,_41,_42);
}
var _43;
if(this._textFreePool.length>0){
_43=this._textFreePool.pop();
_43.setShape({x:x,y:y,text:_3f,align:_3e});
_3d.add(_43);
}else{
_43=_9.createText[_3c](this.chart,_3d,x,y,_3e,_3f,_40,_41);
}
this._textUsePool.push(_43);
return _43;
},createLine:function(_44,_45){
var _46;
if(this.opt.enableCache&&this._lineFreePool.length>0){
_46=this._lineFreePool.pop();
_46.setShape(_45);
_44.add(_46);
}else{
_46=_44.createLine(_45);
}
if(this.opt.enableCache){
this._lineUsePool.push(_46);
}
return _46;
},render:function(dim,_47){
var _48=this._isRtl();
if(!this.dirty||!this.scaler){
return this;
}
var o=this.opt,ta=this.chart.theme.axis,_49=o.leftBottom,_4a=o.rotation%360,_4b,_4c,_4d,_4e=0,_4f,_50,_51,_52,_53,_54,_55=this.chart.theme.axis.tick.labelGap,_56=o.font||(ta.majorTick&&ta.majorTick.font)||(ta.tick&&ta.tick.font),_57=o.titleFont||(ta.title&&ta.title.font),_58=o.fontColor||(ta.majorTick&&ta.majorTick.fontColor)||(ta.tick&&ta.tick.fontColor)||"black",_59=o.titleFontColor||(ta.title&&ta.title.fontColor)||"black",_5a=(o.titleGap==0)?0:o.titleGap||(ta.title&&ta.title.gap)||15,_5b=o.titleOrientation||(ta.title&&ta.title.orientation)||"axis",_5c=this.chart.theme.getTick("major",o),_5d=this.chart.theme.getTick("minor",o),_5e=this.chart.theme.getTick("micro",o),_5f="stroke" in o?o.stroke:ta.stroke,_60=_56?g.normalizedLength(g.splitFontString(_56).size):0,_61=Math.abs(Math.cos(_4a*Math.PI/180)),_62=Math.abs(Math.sin(_4a*Math.PI/180)),_63=_57?g.normalizedLength(g.splitFontString(_57).size):0;
if(typeof _55!="number"){
_55=4;
}
if(_4a<0){
_4a+=360;
}
var _64=this._getMaxLabelSize();
_64=_64&&_64.majLabelW;
if(this.vertical){
_4b={y:dim.height-_47.b};
_4c={y:_47.t};
_4d={y:(dim.height-_47.b+_47.t)/2};
_4f=_60*_62+(_64||0)*_61+_55+Math.max(_5c.length>0?_5c.length:0,_5d.length>0?_5d.length:0)+_63+_5a;
_50={x:0,y:-1};
_53={x:0,y:0};
_51={x:1,y:0};
_52={x:_55,y:0};
switch(_4a){
case 0:
_54="end";
_53.y=_60*0.4;
break;
case 90:
_54="middle";
_53.x=-_60;
break;
case 180:
_54="start";
_53.y=-_60*0.4;
break;
case 270:
_54="middle";
break;
default:
if(_4a<_b){
_54="end";
_53.y=_60*0.4;
}else{
if(_4a<90){
_54="end";
_53.y=_60*0.4;
}else{
if(_4a<(180-_b)){
_54="start";
}else{
if(_4a<(180+_b)){
_54="start";
_53.y=-_60*0.4;
}else{
if(_4a<270){
_54="start";
_53.x=_49?0:_60*0.4;
}else{
if(_4a<(360-_b)){
_54="end";
_53.x=_49?0:_60*0.4;
}else{
_54="end";
_53.y=_60*0.4;
}
}
}
}
}
}
}
if(_49){
_4b.x=_4c.x=_47.l;
_4e=(_5b&&_5b=="away")?90:270;
_4d.x=_47.l-_4f+(_4e==270?_63:0);
_51.x=-1;
_52.x=-_52.x;
}else{
_4b.x=_4c.x=dim.width-_47.r;
_4e=(_5b&&_5b=="axis")?90:270;
_4d.x=dim.width-_47.r+_4f-(_4e==270?0:_63);
switch(_54){
case "start":
_54="end";
break;
case "end":
_54="start";
break;
case "middle":
_53.x+=_60;
break;
}
}
}else{
_4b={x:_47.l};
_4c={x:dim.width-_47.r};
_4d={x:(dim.width-_47.r+_47.l)/2};
_4f=_60*_61+(_64||0)*_62+_55+Math.max(_5c.length>0?_5c.length:0,_5d.length>0?_5d.length:0)+_63+_5a;
_50={x:_48?-1:1,y:0};
_53={x:0,y:0};
_51={x:0,y:1};
_52={x:0,y:_55};
switch(_4a){
case 0:
_54="middle";
_53.y=_60;
break;
case 90:
_54="start";
_53.x=-_60*0.4;
break;
case 180:
_54="middle";
break;
case 270:
_54="end";
_53.x=_60*0.4;
break;
default:
if(_4a<(90-_b)){
_54="start";
_53.y=_49?_60:0;
}else{
if(_4a<(90+_b)){
_54="start";
_53.x=-_60*0.4;
}else{
if(_4a<180){
_54="start";
_53.y=_49?0:-_60;
}else{
if(_4a<(270-_b)){
_54="end";
_53.y=_49?0:-_60;
}else{
if(_4a<(270+_b)){
_54="end";
_53.y=_49?_60*0.4:0;
}else{
_54="end";
_53.y=_49?_60:0;
}
}
}
}
}
}
if(_49){
_4b.y=_4c.y=dim.height-_47.b;
_4e=(_5b&&_5b=="axis")?180:0;
_4d.y=dim.height-_47.b+_4f-(_4e?_63:0);
}else{
_4b.y=_4c.y=_47.t;
_4e=(_5b&&_5b=="away")?180:0;
_4d.y=_47.t-_4f+(_4e?0:_63);
_51.y=-1;
_52.y=-_52.y;
switch(_54){
case "start":
_54="end";
break;
case "end":
_54="start";
break;
case "middle":
_53.y-=_60;
break;
}
}
}
this.cleanGroup();
var s=this.group,c=this.scaler,t=this.ticks,f=_8.getTransformerFromModel(this.scaler),_65=(!o.title||!_4e)&&!_4a&&this.opt.htmlLabels&&!_3("ie")&&!_3("opera")?"html":"gfx",dx=_51.x*_5c.length,dy=_51.y*_5c.length,_66=this._skipInterval;
s.createLine({x1:_4b.x,y1:_4b.y,x2:_4c.x,y2:_4c.y}).setStroke(_5f);
if(o.title){
var _67=_9.createText[_65](this.chart,s,_4d.x,_4d.y,"middle",o.title,_57,_59);
if(_65=="html"){
this.htmlElements.push(_67);
}else{
_67.setTransform(g.matrix.rotategAt(_4e,_4d.x,_4d.y));
}
}
if(t==null){
this.dirty=false;
return this;
}
var rel=(t.major.length>0)?(t.major[0].value-this._majorStart)/c.major.tick:0;
var _68=this.opt.majorLabels;
_2.forEach(t.major,function(_69,i){
var _6a=f(_69.value),_6b,x=(_48?_4c.x:_4b.x)+_50.x*_6a,y=_4b.y+_50.y*_6a;
i+=rel;
this.createLine(s,{x1:x,y1:y,x2:x+dx,y2:y+dy}).setStroke(_5c);
if(_69.label&&(!_66||(i-(1+_66))%(1+_66)==0)){
var _6c=o.maxLabelCharCount?this.getTextWithLimitCharCount(_69.label,_56,o.maxLabelCharCount):{text:_69.label,truncated:false};
_6c=o.maxLabelSize?this.getTextWithLimitLength(_6c.text,_56,o.maxLabelSize,_6c.truncated):_6c;
_6b=this.createText(_65,s,x+(_5c.length>0?dx:0)+_52.x+(_4a?0:_53.x),y+(_5c.length>0?dy:0)+_52.y+(_4a?0:_53.y),_54,_6c.text,_56,_58);
if(_6c.truncated){
this.chart.formatTruncatedLabel(_6b,_69.label,_65);
}
_6c.truncated&&this.labelTooltip(_6b,this.chart,_69.label,_6c.text,_56,_65);
if(_65=="html"){
this.htmlElements.push(_6b);
}else{
if(_4a){
_6b.setTransform([{dx:_53.x,dy:_53.y},g.matrix.rotategAt(_4a,x+(_5c.length>0?dx:0)+_52.x,y+(_5c.length>0?dy:0)+_52.y)]);
}
}
}
},this);
dx=_51.x*_5d.length;
dy=_51.y*_5d.length;
_68=this.opt.minorLabels&&c.minMinorStep<=c.minor.tick*c.bounds.scale;
_2.forEach(t.minor,function(_6d){
var _6e=f(_6d.value),_6f,x=(_48?_4c.x:_4b.x)+_50.x*_6e,y=_4b.y+_50.y*_6e;
this.createLine(s,{x1:x,y1:y,x2:x+dx,y2:y+dy}).setStroke(_5d);
if(_68&&_6d.label){
var _70=o.maxLabelCharCount?this.getTextWithLimitCharCount(_6d.label,_56,o.maxLabelCharCount):{text:_6d.label,truncated:false};
_70=o.maxLabelSize?this.getTextWithLimitLength(_70.text,_56,o.maxLabelSize,_70.truncated):_70;
_6f=this.createText(_65,s,x+(_5d.length>0?dx:0)+_52.x+(_4a?0:_53.x),y+(_5d.length>0?dy:0)+_52.y+(_4a?0:_53.y),_54,_70.text,_56,_58);
if(_70.truncated){
this.chart.formatTruncatedLabel(_6f,_6d.label,_65);
}
_70.truncated&&this.labelTooltip(_6f,this.chart,_6d.label,_70.text,_56,_65);
if(_65=="html"){
this.htmlElements.push(_6f);
}else{
if(_4a){
_6f.setTransform([{dx:_53.x,dy:_53.y},g.matrix.rotategAt(_4a,x+(_5d.length>0?dx:0)+_52.x,y+(_5d.length>0?dy:0)+_52.y)]);
}
}
}
},this);
dx=_51.x*_5e.length;
dy=_51.y*_5e.length;
_2.forEach(t.micro,function(_71){
var _72=f(_71.value),x=_4b.x+_50.x*_72,y=_4b.y+_50.y*_72;
this.createLine(s,{x1:x,y1:y,x2:x+dx,y2:y+dy}).setStroke(_5e);
},this);
this.dirty=false;
return this;
},labelTooltip:function(_73,_74,_75,_76,_77,_78){
var _79=["dijit/Tooltip"];
var _7a={type:"rect"},_7b=["above","below"],_7c=g._base._getTextBox(_76,{font:_77}).w||0,_7d=_77?g.normalizedLength(g.splitFontString(_77).size):0;
if(_78=="html"){
_1.mixin(_7a,_6.position(_73.firstChild,true));
_7a.width=Math.ceil(_7c);
_7a.height=Math.ceil(_7d);
this._events.push({shape:dojo,handle:_5.connect(_73.firstChild,"onmouseover",this,function(e){
require(_79,function(_7e){
_7e.show(_75,_7a,_7b);
});
})});
this._events.push({shape:dojo,handle:_5.connect(_73.firstChild,"onmouseout",this,function(e){
require(_79,function(_7f){
_7f.hide(_7a);
});
})});
}else{
var shp=_73.getShape(),lt=_74.getCoords();
_7a=_1.mixin(_7a,{x:shp.x-_7c/2,y:shp.y});
_7a.x+=lt.x;
_7a.y+=lt.y;
_7a.x=Math.round(_7a.x);
_7a.y=Math.round(_7a.y);
_7a.width=Math.ceil(_7c);
_7a.height=Math.ceil(_7d);
this._events.push({shape:_73,handle:_73.connect("onmouseenter",this,function(e){
require(_79,function(_80){
_80.show(_75,_7a,_7b);
});
})});
this._events.push({shape:_73,handle:_73.connect("onmouseleave",this,function(e){
require(_79,function(_81){
_81.hide(_7a);
});
})});
}
},_isRtl:function(){
return false;
}});
return _3("dojo-bidi")?_4("dojox.charting.axis2d.Default",[_c,_a]):_c;
});
