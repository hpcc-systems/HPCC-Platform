//>>built
define("dojox/mobile/Button",["dojo/_base/array","dojo/_base/declare","dojo/dom-class","dojo/dom-construct","dijit/_WidgetBase","dijit/form/_ButtonMixin","dijit/form/_FormWidgetMixin","dojo/has","dojo/has!dojo-bidi?dojox/mobile/bidi/Button"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9){
var _a=_2(_8("dojo-bidi")?"dojox.mobile.NonBidiButton":"dojox.mobile.Button",[_5,_7,_6],{baseClass:"mblButton",_setTypeAttr:null,duration:1000,_onClick:function(e){
var _b=this.inherited(arguments);
if(_b&&this.duration>=0){
var _c=this.focusNode||this.domNode;
var _d=(this.baseClass+" "+this["class"]).split(" ");
_d=_1.map(_d,function(c){
return c+"Selected";
});
_3.add(_c,_d);
this.defer(function(){
_3.remove(_c,_d);
},this.duration);
}
return _b;
},isFocusable:function(){
return false;
},buildRendering:function(){
if(!this.srcNodeRef){
this.srcNodeRef=_4.create("button",{"type":this.type});
}else{
if(this._cv){
var n=this.srcNodeRef.firstChild;
if(n&&n.nodeType===3){
n.nodeValue=this._cv(n.nodeValue);
}
}
}
this.inherited(arguments);
this.focusNode=this.domNode;
},postCreate:function(){
this.inherited(arguments);
this.connect(this.domNode,"onclick","_onClick");
},_setLabelAttr:function(_e){
this.inherited(arguments,[this._cv?this._cv(_e):_e]);
}});
return _8("dojo-bidi")?_2("dojox.mobile.Button",[_a,_9]):_a;
});
