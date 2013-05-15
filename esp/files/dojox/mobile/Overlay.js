//>>built
define("dojox/mobile/Overlay",["dojo/_base/declare","dojo/_base/lang","dojo/sniff","dojo/_base/window","dojo/dom-class","dojo/dom-geometry","dojo/dom-style","dojo/window","dijit/_WidgetBase","dojo/_base/array","dijit/registry","dojo/touch","./_css3"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9,_a,_b,_c,_d){
return _1("dojox.mobile.Overlay",_9,{baseClass:"mblOverlay mblOverlayHidden",buildRendering:function(){
this.inherited(arguments);
if(!this.containerNode){
this.containerNode=this.domNode;
}
},_reposition:function(){
var _e=_6.position(this.domNode);
var vp=_8.getBox();
if((_e.y+_e.h)!=vp.h||(_7.get(this.domNode,"position")!="absolute"&&_3("android")<3)){
_e.y=vp.t+vp.h-_e.h;
_7.set(this.domNode,{position:"absolute",top:_e.y+"px",bottom:"auto"});
}
return _e;
},show:function(_f){
_a.forEach(_b.findWidgets(this.domNode),function(w){
if(w&&w.height=="auto"&&typeof w.resize=="function"){
w.resize();
}
});
var _10=this._reposition();
if(_f){
var _11=_6.position(_f);
if(_10.y<_11.y){
_4.global.scrollBy(0,_11.y+_11.h-_10.y);
this._reposition();
}
}
var _12=this.domNode;
_5.replace(_12,["mblCoverv","mblIn"],["mblOverlayHidden","mblRevealv","mblOut","mblReverse","mblTransition"]);
this.defer(function(){
var _13=this.connect(_12,_d.name("transitionEnd"),function(){
this.disconnect(_13);
_5.remove(_12,["mblCoverv","mblIn","mblTransition"]);
this._reposition();
});
_5.add(_12,"mblTransition");
},100);
var _14=false;
this._moveHandle=this.connect(_4.doc.documentElement,_c.move,function(){
_14=true;
});
this._repositionTimer=setInterval(_2.hitch(this,function(){
if(_14){
_14=false;
return;
}
this._reposition();
}),50);
return _10;
},hide:function(){
var _15=this.domNode;
if(this._moveHandle){
this.disconnect(this._moveHandle);
this._moveHandle=null;
clearInterval(this._repositionTimer);
this._repositionTimer=null;
}
if(_3("css3-animations")){
_5.replace(_15,["mblRevealv","mblOut","mblReverse"],["mblCoverv","mblIn","mblOverlayHidden","mblTransition"]);
this.defer(function(){
var _16=this.connect(_15,_d.name("transitionEnd"),function(){
this.disconnect(_16);
_5.replace(_15,["mblOverlayHidden"],["mblRevealv","mblOut","mblReverse","mblTransition"]);
});
_5.add(_15,"mblTransition");
},100);
}else{
_5.replace(_15,["mblOverlayHidden"],["mblCoverv","mblIn","mblRevealv","mblOut","mblReverse"]);
}
},onBlur:function(e){
return false;
}});
});
