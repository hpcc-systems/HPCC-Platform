//>>built
define("dojox/gfx/shape",["./_base","dojo/_base/lang","dojo/_base/declare","dojo/_base/kernel","dojo/_base/sniff","dojo/on","dojo/_base/array","dojo/dom-construct","dojo/_base/Color","./matrix"],function(g,_1,_2,_3,_4,on,_5,_6,_7,_8){
var _9=g.shape={};
_9.Shape=_2("dojox.gfx.shape.Shape",null,{constructor:function(){
this.rawNode=null;
this.shape=null;
this.matrix=null;
this.fillStyle=null;
this.strokeStyle=null;
this.bbox=null;
this.parent=null;
this.parentMatrix=null;
if(_4("gfxRegistry")){
var _a=_9.register(this);
this.getUID=function(){
return _a;
};
}
},destroy:function(){
if(_4("gfxRegistry")){
_9.dispose(this);
}
if(this.rawNode&&"__gfxObject__" in this.rawNode){
this.rawNode.__gfxObject__=null;
}
this.rawNode=null;
},getNode:function(){
return this.rawNode;
},getShape:function(){
return this.shape;
},getTransform:function(){
return this.matrix;
},getFill:function(){
return this.fillStyle;
},getStroke:function(){
return this.strokeStyle;
},getParent:function(){
return this.parent;
},getBoundingBox:function(){
return this.bbox;
},getTransformedBoundingBox:function(){
var b=this.getBoundingBox();
if(!b){
return null;
}
var m=this._getRealMatrix(),gm=_8;
return [gm.multiplyPoint(m,b.x,b.y),gm.multiplyPoint(m,b.x+b.width,b.y),gm.multiplyPoint(m,b.x+b.width,b.y+b.height),gm.multiplyPoint(m,b.x,b.y+b.height)];
},getEventSource:function(){
return this.rawNode;
},setClip:function(_b){
this.clip=_b;
},getClip:function(){
return this.clip;
},setShape:function(_c){
this.shape=g.makeParameters(this.shape,_c);
this.bbox=null;
return this;
},setFill:function(_d){
if(!_d){
this.fillStyle=null;
return this;
}
var f=null;
if(typeof (_d)=="object"&&"type" in _d){
switch(_d.type){
case "linear":
f=g.makeParameters(g.defaultLinearGradient,_d);
break;
case "radial":
f=g.makeParameters(g.defaultRadialGradient,_d);
break;
case "pattern":
f=g.makeParameters(g.defaultPattern,_d);
break;
}
}else{
f=g.normalizeColor(_d);
}
this.fillStyle=f;
return this;
},setStroke:function(_e){
if(!_e){
this.strokeStyle=null;
return this;
}
if(typeof _e=="string"||_1.isArray(_e)||_e instanceof _7){
_e={color:_e};
}
var s=this.strokeStyle=g.makeParameters(g.defaultStroke,_e);
s.color=g.normalizeColor(s.color);
return this;
},setTransform:function(_f){
this.matrix=_8.clone(_f?_8.normalize(_f):_8.identity);
return this._applyTransform();
},_applyTransform:function(){
return this;
},moveToFront:function(){
var p=this.getParent();
if(p){
p._moveChildToFront(this);
this._moveToFront();
}
return this;
},moveToBack:function(){
var p=this.getParent();
if(p){
p._moveChildToBack(this);
this._moveToBack();
}
return this;
},_moveToFront:function(){
},_moveToBack:function(){
},applyRightTransform:function(_10){
return _10?this.setTransform([this.matrix,_10]):this;
},applyLeftTransform:function(_11){
return _11?this.setTransform([_11,this.matrix]):this;
},applyTransform:function(_12){
return _12?this.setTransform([this.matrix,_12]):this;
},removeShape:function(_13){
if(this.parent){
this.parent.remove(this,_13);
}
return this;
},_setParent:function(_14,_15){
this.parent=_14;
return this._updateParentMatrix(_15);
},_updateParentMatrix:function(_16){
this.parentMatrix=_16?_8.clone(_16):null;
return this._applyTransform();
},_getRealMatrix:function(){
var m=this.matrix;
var p=this.parent;
while(p){
if(p.matrix){
m=_8.multiply(p.matrix,m);
}
p=p.parent;
}
return m;
}});
_9._eventsProcessing={on:function(_17,_18){
return on(this.getEventSource(),_17,_9.fixCallback(this,g.fixTarget,_18));
},connect:function(_19,_1a,_1b){
if(_19.substring(0,2)=="on"){
_19=_19.substring(2);
}
return this.on(_19,_1b?_1.hitch(_1a,_1b):_1a);
},disconnect:function(_1c){
return _1c.remove();
}};
_9.fixCallback=function(_1d,_1e,_1f,_20){
if(!_20){
_20=_1f;
_1f=null;
}
if(_1.isString(_20)){
_1f=_1f||_3.global;
if(!_1f[_20]){
throw (["dojox.gfx.shape.fixCallback: scope[\"",_20,"\"] is null (scope=\"",_1f,"\")"].join(""));
}
return function(e){
return _1e(e,_1d)?_1f[_20].apply(_1f,arguments||[]):undefined;
};
}
return !_1f?function(e){
return _1e(e,_1d)?_20.apply(_1f,arguments):undefined;
}:function(e){
return _1e(e,_1d)?_20.apply(_1f,arguments||[]):undefined;
};
};
_1.extend(_9.Shape,_9._eventsProcessing);
_9.Container={_init:function(){
this.children=[];
this._batch=0;
},openBatch:function(){
return this;
},closeBatch:function(){
return this;
},add:function(_21){
var _22=_21.getParent();
if(_22){
_22.remove(_21,true);
}
this.children.push(_21);
return _21._setParent(this,this._getRealMatrix());
},remove:function(_23,_24){
for(var i=0;i<this.children.length;++i){
if(this.children[i]==_23){
if(_24){
}else{
_23.parent=null;
_23.parentMatrix=null;
}
this.children.splice(i,1);
break;
}
}
return this;
},clear:function(_25){
var _26;
for(var i=0;i<this.children.length;++i){
_26=this.children[i];
_26.parent=null;
_26.parentMatrix=null;
if(_25){
_26.destroy();
}
}
this.children=[];
return this;
},getBoundingBox:function(){
if(this.children){
var _27=null;
_5.forEach(this.children,function(_28){
var bb=_28.getBoundingBox();
if(bb){
var ct=_28.getTransform();
if(ct){
bb=_8.multiplyRectangle(ct,bb);
}
if(_27){
_27.x=Math.min(_27.x,bb.x);
_27.y=Math.min(_27.y,bb.y);
_27.endX=Math.max(_27.endX,bb.x+bb.width);
_27.endY=Math.max(_27.endY,bb.y+bb.height);
}else{
_27={x:bb.x,y:bb.y,endX:bb.x+bb.width,endY:bb.y+bb.height};
}
}
});
if(_27){
_27.width=_27.endX-_27.x;
_27.height=_27.endY-_27.y;
}
return _27;
}
return null;
},_moveChildToFront:function(_29){
for(var i=0;i<this.children.length;++i){
if(this.children[i]==_29){
this.children.splice(i,1);
this.children.push(_29);
break;
}
}
return this;
},_moveChildToBack:function(_2a){
for(var i=0;i<this.children.length;++i){
if(this.children[i]==_2a){
this.children.splice(i,1);
this.children.unshift(_2a);
break;
}
}
return this;
}};
_9.Surface=_2("dojox.gfx.shape.Surface",null,{constructor:function(){
this.rawNode=null;
this._parent=null;
this._nodes=[];
this._events=[];
},destroy:function(){
_5.forEach(this._nodes,_6.destroy);
this._nodes=[];
_5.forEach(this._events,function(h){
if(h){
h.remove();
}
});
this._events=[];
this.rawNode=null;
if(_4("ie")){
while(this._parent.lastChild){
_6.destroy(this._parent.lastChild);
}
}else{
this._parent.innerHTML="";
}
this._parent=null;
},getEventSource:function(){
return this.rawNode;
},_getRealMatrix:function(){
return null;
},isLoaded:true,onLoad:function(_2b){
},whenLoaded:function(_2c,_2d){
var f=_1.hitch(_2c,_2d);
if(this.isLoaded){
f(this);
}else{
on.once(this,"load",function(_2e){
f(_2e);
});
}
}});
_1.extend(_9.Surface,_9._eventsProcessing);
_9.Rect=_2("dojox.gfx.shape.Rect",_9.Shape,{constructor:function(_2f){
this.shape=g.getDefault("Rect");
this.rawNode=_2f;
},getBoundingBox:function(){
return this.shape;
}});
_9.Ellipse=_2("dojox.gfx.shape.Ellipse",_9.Shape,{constructor:function(_30){
this.shape=g.getDefault("Ellipse");
this.rawNode=_30;
},getBoundingBox:function(){
if(!this.bbox){
var _31=this.shape;
this.bbox={x:_31.cx-_31.rx,y:_31.cy-_31.ry,width:2*_31.rx,height:2*_31.ry};
}
return this.bbox;
}});
_9.Circle=_2("dojox.gfx.shape.Circle",_9.Shape,{constructor:function(_32){
this.shape=g.getDefault("Circle");
this.rawNode=_32;
},getBoundingBox:function(){
if(!this.bbox){
var _33=this.shape;
this.bbox={x:_33.cx-_33.r,y:_33.cy-_33.r,width:2*_33.r,height:2*_33.r};
}
return this.bbox;
}});
_9.Line=_2("dojox.gfx.shape.Line",_9.Shape,{constructor:function(_34){
this.shape=g.getDefault("Line");
this.rawNode=_34;
},getBoundingBox:function(){
if(!this.bbox){
var _35=this.shape;
this.bbox={x:Math.min(_35.x1,_35.x2),y:Math.min(_35.y1,_35.y2),width:Math.abs(_35.x2-_35.x1),height:Math.abs(_35.y2-_35.y1)};
}
return this.bbox;
}});
_9.Polyline=_2("dojox.gfx.shape.Polyline",_9.Shape,{constructor:function(_36){
this.shape=g.getDefault("Polyline");
this.rawNode=_36;
},setShape:function(_37,_38){
if(_37&&_37 instanceof Array){
this.inherited(arguments,[{points:_37}]);
if(_38&&this.shape.points.length){
this.shape.points.push(this.shape.points[0]);
}
}else{
this.inherited(arguments,[_37]);
}
return this;
},_normalizePoints:function(){
var p=this.shape.points,l=p&&p.length;
if(l&&typeof p[0]=="number"){
var _39=[];
for(var i=0;i<l;i+=2){
_39.push({x:p[i],y:p[i+1]});
}
this.shape.points=_39;
}
},getBoundingBox:function(){
if(!this.bbox&&this.shape.points.length){
var p=this.shape.points;
var l=p.length;
var t=p[0];
var _3a={l:t.x,t:t.y,r:t.x,b:t.y};
for(var i=1;i<l;++i){
t=p[i];
if(_3a.l>t.x){
_3a.l=t.x;
}
if(_3a.r<t.x){
_3a.r=t.x;
}
if(_3a.t>t.y){
_3a.t=t.y;
}
if(_3a.b<t.y){
_3a.b=t.y;
}
}
this.bbox={x:_3a.l,y:_3a.t,width:_3a.r-_3a.l,height:_3a.b-_3a.t};
}
return this.bbox;
}});
_9.Image=_2("dojox.gfx.shape.Image",_9.Shape,{constructor:function(_3b){
this.shape=g.getDefault("Image");
this.rawNode=_3b;
},getBoundingBox:function(){
return this.shape;
},setStroke:function(){
return this;
},setFill:function(){
return this;
}});
_9.Text=_2(_9.Shape,{constructor:function(_3c){
this.fontStyle=null;
this.shape=g.getDefault("Text");
this.rawNode=_3c;
},getFont:function(){
return this.fontStyle;
},setFont:function(_3d){
this.fontStyle=typeof _3d=="string"?g.splitFontString(_3d):g.makeParameters(g.defaultFont,_3d);
this._setFont();
return this;
},getBoundingBox:function(){
var _3e=null,s=this.getShape();
if(s.text){
_3e=g._base._computeTextBoundingBox(this);
}
return _3e;
}});
_9.Creator={createShape:function(_3f){
switch(_3f.type){
case g.defaultPath.type:
return this.createPath(_3f);
case g.defaultRect.type:
return this.createRect(_3f);
case g.defaultCircle.type:
return this.createCircle(_3f);
case g.defaultEllipse.type:
return this.createEllipse(_3f);
case g.defaultLine.type:
return this.createLine(_3f);
case g.defaultPolyline.type:
return this.createPolyline(_3f);
case g.defaultImage.type:
return this.createImage(_3f);
case g.defaultText.type:
return this.createText(_3f);
case g.defaultTextPath.type:
return this.createTextPath(_3f);
}
return null;
},createGroup:function(){
return this.createObject(g.Group);
},createRect:function(_40){
return this.createObject(g.Rect,_40);
},createEllipse:function(_41){
return this.createObject(g.Ellipse,_41);
},createCircle:function(_42){
return this.createObject(g.Circle,_42);
},createLine:function(_43){
return this.createObject(g.Line,_43);
},createPolyline:function(_44){
return this.createObject(g.Polyline,_44);
},createImage:function(_45){
return this.createObject(g.Image,_45);
},createText:function(_46){
return this.createObject(g.Text,_46);
},createPath:function(_47){
return this.createObject(g.Path,_47);
},createTextPath:function(_48){
return this.createObject(g.TextPath,{}).setText(_48);
},createObject:function(_49,_4a){
return null;
}};
return _9;
});
