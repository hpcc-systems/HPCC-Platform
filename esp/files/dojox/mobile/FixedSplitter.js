//>>built
define("dojox/mobile/FixedSplitter",["dojo/_base/array","dojo/_base/declare","dojo/_base/window","dojo/dom-class","dojo/dom-geometry","dijit/_Contained","dijit/_Container","dijit/_WidgetBase","dojo/has"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9){
return _2("dojox.mobile.FixedSplitter",[_8,_7,_6],{orientation:"H",variablePane:-1,screenSizeAware:false,screenSizeAwareClass:"dojox/mobile/ScreenSizeAware",baseClass:"mblFixedSplitter",startup:function(){
if(this._started){
return;
}
_4.add(this.domNode,this.baseClass+this.orientation);
var _a=this.getParent(),f;
if(!_a||!_a.resize){
var _b=this;
f=function(){
_b.defer(function(){
_b.resize();
});
};
}
if(this.screenSizeAware){
require([this.screenSizeAwareClass],function(_c){
_c.getInstance();
f&&f();
});
}else{
f&&f();
}
this.inherited(arguments);
},resize:function(){
var wh=this.orientation==="H"?"w":"h",tl=this.orientation==="H"?"l":"t",_d={},_e={},i,c,h,a=[],_f=0,_10=0,_11=_1.filter(this.domNode.childNodes,function(_12){
return _12.nodeType==1;
}),idx=this.variablePane==-1?_11.length-1:this.variablePane;
for(i=0;i<_11.length;i++){
if(i!=idx){
a[i]=_5.getMarginBox(_11[i])[wh];
_10+=a[i];
}
}
if(this.orientation=="V"){
if(this.domNode.parentNode.tagName=="BODY"){
if(_1.filter(_3.body().childNodes,function(_13){
return _13.nodeType==1;
}).length==1){
h=(_3.global.innerHeight||_3.doc.documentElement.clientHeight);
}
}
}
var l=(h||_5.getMarginBox(this.domNode)[wh])-_10;
_e[wh]=a[idx]=l;
c=_11[idx];
_5.setMarginBox(c,_e);
c.style[this.orientation==="H"?"height":"width"]="";
if(_9("dojo-bidi")&&(this.orientation=="H"&&!this.isLeftToRight())){
_f=l;
for(i=_11.length-1;i>=0;i--){
c=_11[i];
_d[tl]=l-_f;
_5.setMarginBox(c,_d);
c.style[this.orientation==="H"?"top":"left"]="";
_f-=a[i];
}
}else{
for(i=0;i<_11.length;i++){
c=_11[i];
_d[tl]=_f;
_5.setMarginBox(c,_d);
c.style[this.orientation==="H"?"top":"left"]="";
_f+=a[i];
}
}
_1.forEach(this.getChildren(),function(_14){
if(_14.resize){
_14.resize();
}
});
},_setOrientationAttr:function(_15){
var s=this.baseClass;
_4.replace(this.domNode,s+_15,s+this.orientation);
this.orientation=_15;
if(this._started){
this.resize();
}
}});
});
