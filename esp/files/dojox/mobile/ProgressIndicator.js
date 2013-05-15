//>>built
define("dojox/mobile/ProgressIndicator",["dojo/_base/config","dojo/_base/declare","dojo/_base/lang","dojo/dom-class","dojo/dom-construct","dojo/dom-geometry","dojo/dom-style","dojo/has","dijit/_Contained","dijit/_WidgetBase","./_css3"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9,_a,_b){
var _c=_2("dojox.mobile.ProgressIndicator",[_a,_9],{interval:100,size:40,removeOnStop:true,startSpinning:false,center:true,colors:null,baseClass:"mblProgressIndicator",constructor:function(){
this.colors=[];
this._bars=[];
},buildRendering:function(){
this.inherited(arguments);
if(this.center){
_4.add(this.domNode,"mblProgressIndicatorCenter");
}
this.containerNode=_5.create("div",{className:"mblProgContainer"},this.domNode);
this.spinnerNode=_5.create("div",null,this.containerNode);
for(var i=0;i<12;i++){
var _d=_5.create("div",{className:"mblProg mblProg"+i},this.spinnerNode);
this._bars.push(_d);
}
this.scale(this.size);
if(this.startSpinning){
this.start();
}
},scale:function(_e){
var _f=_e/40;
_7.set(this.containerNode,_b.add({},{transform:"scale("+_f+")",transformOrigin:"0 0"}));
_6.setMarginBox(this.domNode,{w:_e,h:_e});
_6.setMarginBox(this.containerNode,{w:_e/_f,h:_e/_f});
},start:function(){
if(this.imageNode){
var img=this.imageNode;
var l=Math.round((this.containerNode.offsetWidth-img.offsetWidth)/2);
var t=Math.round((this.containerNode.offsetHeight-img.offsetHeight)/2);
img.style.margin=t+"px "+l+"px";
return;
}
var _10=0;
var _11=this;
var n=12;
this.timer=setInterval(function(){
_10--;
_10=_10<0?n-1:_10;
var c=_11.colors;
for(var i=0;i<n;i++){
var idx=(_10+i)%n;
if(c[idx]){
_11._bars[i].style.backgroundColor=c[idx];
}else{
_4.replace(_11._bars[i],"mblProg"+idx+"Color","mblProg"+(idx===n-1?0:idx+1)+"Color");
}
}
},this.interval);
},stop:function(){
if(this.timer){
clearInterval(this.timer);
}
this.timer=null;
if(this.removeOnStop&&this.domNode&&this.domNode.parentNode){
this.domNode.parentNode.removeChild(this.domNode);
}
},setImage:function(_12){
if(_12){
this.imageNode=_5.create("img",{src:_12},this.containerNode);
this.spinnerNode.style.display="none";
}else{
if(this.imageNode){
this.containerNode.removeChild(this.imageNode);
this.imageNode=null;
}
this.spinnerNode.style.display="";
}
},destroy:function(){
this.inherited(arguments);
if(this===_c._instance){
_c._instance=null;
}
}});
_c._instance=null;
_c.getInstance=function(_13){
if(!_c._instance){
_c._instance=new _c(_13);
}
return _c._instance;
};
return _c;
});
