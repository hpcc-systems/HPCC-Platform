//>>built
define("dojox/mobile/Slider",["dojo/_base/array","dojo/_base/connect","dojo/_base/declare","dojo/_base/lang","dojo/_base/window","dojo/sniff","dojo/dom-class","dojo/dom-construct","dojo/dom-geometry","dojo/dom-style","dojo/keys","dojo/touch","dijit/_WidgetBase","dijit/form/_FormValueMixin"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9,_a,_b,_c,_d,_e){
return _3("dojox.mobile.Slider",[_d,_e],{value:0,min:0,max:100,step:1,baseClass:"mblSlider",flip:false,orientation:"auto",halo:"8pt",buildRendering:function(){
if(!this.templateString){
this.focusNode=this.domNode=_8.create("div",{});
this.valueNode=_8.create("input",(this.srcNodeRef&&this.srcNodeRef.name)?{type:"hidden",name:this.srcNodeRef.name}:{type:"hidden"},this.domNode,"last");
var _f=_8.create("div",{style:{position:"relative",height:"100%",width:"100%"}},this.domNode,"last");
this.progressBar=_8.create("div",{style:{position:"absolute"},"class":"mblSliderProgressBar"},_f,"last");
this.touchBox=_8.create("div",{style:{position:"absolute"},"class":"mblSliderTouchBox"},_f,"last");
this.handle=_8.create("div",{style:{position:"absolute"},"class":"mblSliderHandle"},_f,"last");
}
this.inherited(arguments);
if(typeof this.domNode.style.msTouchAction!="undefined"){
this.domNode.style.msTouchAction="none";
}
},_setValueAttr:function(_10,_11){
_10=Math.max(Math.min(_10,this.max),this.min);
var _12=(this.value-this.min)*100/(this.max-this.min);
this.valueNode.value=_10;
this.inherited(arguments);
if(!this._started){
return;
}
this.focusNode.setAttribute("aria-valuenow",_10);
var _13=(_10-this.min)*100/(this.max-this.min);
var _14=this.orientation!="V";
if(_11===true){
_7.add(this.handle,"mblSliderTransition");
_7.add(this.progressBar,"mblSliderTransition");
}else{
_7.remove(this.handle,"mblSliderTransition");
_7.remove(this.progressBar,"mblSliderTransition");
}
_a.set(this.handle,this._attrs.handleLeft,(this._reversed?(100-_13):_13)+"%");
_a.set(this.progressBar,this._attrs.width,_13+"%");
},postCreate:function(){
this.inherited(arguments);
function _15(e){
function _16(e){
_2b=_17?e[this._attrs.pageX]:(e.touches?e.touches[0][this._attrs.pageX]:e[this._attrs.clientX]);
_2c=_2b-_18;
_2c=Math.min(Math.max(_2c,0),_19);
var _1a=this.step?((this.max-this.min)/this.step):_19;
if(_1a<=1||_1a==Infinity){
_1a=_19;
}
var _1b=Math.round(_2c*_1a/_19);
_22=(this.max-this.min)*_1b/_1a;
_22=this._reversed?(this.max-_22):(this.min+_22);
};
function _1c(e){
e.preventDefault();
_4.hitch(this,_16)(e);
this.set("value",_22,false);
};
function _1d(e){
e.preventDefault();
_1.forEach(_1e,_4.hitch(this,"disconnect"));
_1e=[];
this.set("value",this.value,true);
};
e.preventDefault();
var _17=e.type=="mousedown";
var box=_9.position(_1f,false);
var _20=_6("ie")?1:(_a.get(_5.body(),"zoom")||1);
if(isNaN(_20)){
_20=1;
}
var _21=_6("ie")?1:(_a.get(_1f,"zoom")||1);
if(isNaN(_21)){
_21=1;
}
var _18=box[this._attrs.x]*_21*_20+_9.docScroll()[this._attrs.x];
var _19=box[this._attrs.w]*_21*_20;
_4.hitch(this,_16)(e);
if(e.target==this.touchBox){
this.set("value",_22,true);
}
_1.forEach(_1e,_2.disconnect);
var _23=_5.doc.documentElement;
var _1e=[this.connect(_23,_c.move,_1c),this.connect(_23,_c.release,_1d)];
};
function _24(e){
if(this.disabled||this.readOnly||e.altKey||e.ctrlKey||e.metaKey){
return;
}
var _25=this.step,_26=1,_27;
switch(e.keyCode){
case _b.HOME:
_27=this.min;
break;
case _b.END:
_27=this.max;
break;
case _b.RIGHT_ARROW:
_26=-1;
case _b.LEFT_ARROW:
_27=this.value+_26*((_28&&_29)?_25:-_25);
break;
case _b.DOWN_ARROW:
_26=-1;
case _b.UP_ARROW:
_27=this.value+_26*((!_28||_29)?_25:-_25);
break;
default:
return;
}
e.preventDefault();
this._setValueAttr(_27,false);
};
function _2a(e){
if(this.disabled||this.readOnly||e.altKey||e.ctrlKey||e.metaKey){
return;
}
this._setValueAttr(this.value,true);
};
var _2b,_2c,_22,_1f=this.domNode;
if(this.orientation=="auto"){
this.orientation=_1f.offsetHeight<=_1f.offsetWidth?"H":"V";
}
_7.add(this.domNode,_1.map(this.baseClass.split(" "),_4.hitch(this,function(c){
return c+this.orientation;
})));
var _29=this.orientation!="V",ltr=_29?this.isLeftToRight():false,_28=!!this.flip;
this._reversed=!((_29&&((ltr&&!_28)||(!ltr&&_28)))||(!_29&&_28));
this._attrs=_29?{x:"x",w:"w",l:"l",r:"r",pageX:"pageX",clientX:"clientX",handleLeft:"left",left:this._reversed?"right":"left",width:"width"}:{x:"y",w:"h",l:"t",r:"b",pageX:"pageY",clientX:"clientY",handleLeft:"top",left:this._reversed?"bottom":"top",width:"height"};
this.progressBar.style[this._attrs.left]="0px";
this.connect(this.touchBox,_c.press,_15);
this.connect(this.handle,_c.press,_15);
this.connect(this.domNode,"onkeypress",_24);
this.connect(this.domNode,"onkeyup",_2a);
this.startup();
this.set("value",this.value);
}});
});
