//>>built
define("dojox/mobile/Switch",["dojo/_base/array","dojo/_base/connect","dojo/_base/declare","dojo/_base/event","dojo/_base/window","dojo/dom-class","dojo/dom-construct","dojo/dom-style","dojo/dom-attr","dojo/touch","dijit/_Contained","dijit/_WidgetBase","./sniff","./_maskUtils","./common","dojo/has!dojo-bidi?dojox/mobile/bidi/Switch"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9,_a,_b,_c,_d,_e,dm,_f){
var _10=_3(_d("dojo-bidi")?"dojox.mobile.NonBidiSwitch":"dojox.mobile.Switch",[_c,_b],{value:"on",name:"",leftLabel:"ON",rightLabel:"OFF",shape:"mblSwDefaultShape",tabIndex:"0",_setTabIndexAttr:"",baseClass:"mblSwitch",role:"",buildRendering:function(){
if(!this.templateString){
this.domNode=(this.srcNodeRef&&this.srcNodeRef.tagName==="SPAN")?this.srcNodeRef:_7.create("span");
}
if(typeof this.domNode.style.msTouchAction!="undefined"){
this.domNode.style.msTouchAction="none";
}
this.inherited(arguments);
if(!this.templateString){
var c=(this.srcNodeRef&&this.srcNodeRef.className)||this.className||this["class"];
if((c=c.match(/mblSw.*Shape\d*/))){
this.shape=c;
}
_6.add(this.domNode,this.shape);
var _11=this.name?" name=\""+this.name+"\"":"";
this.domNode.innerHTML="<div class=\"mblSwitchInner\">"+"<div class=\"mblSwitchBg mblSwitchBgLeft\">"+"<div class=\"mblSwitchText mblSwitchTextLeft\"></div>"+"</div>"+"<div class=\"mblSwitchBg mblSwitchBgRight\">"+"<div class=\"mblSwitchText mblSwitchTextRight\"></div>"+"</div>"+"<div class=\"mblSwitchKnob\"></div>"+"<input type=\"hidden\""+_11+"></div>"+"</div>";
var n=this.inner=this.domNode.firstChild;
this.left=n.childNodes[0];
this.right=n.childNodes[1];
this.knob=n.childNodes[2];
this.input=n.childNodes[3];
}
_9.set(this.domNode,"role","checkbox");
_9.set(this.domNode,"aria-checked",(this.value==="on")?"true":"false");
this.switchNode=this.domNode;
if(_d("windows-theme")){
var _12=_7.create("div",{className:"mblSwitchContainer"});
this.labelNode=_7.create("label",{"class":"mblSwitchLabel","for":this.id},_12);
_12.appendChild(this.domNode.cloneNode(true));
this.domNode=_12;
this.focusNode=_12.childNodes[1];
this.labelNode.innerHTML=(this.value=="off")?this.rightLabel:this.leftLabel;
this.switchNode=this.domNode.childNodes[1];
var _13=this.inner=this.domNode.childNodes[1].firstChild;
this.left=_13.childNodes[0];
this.right=_13.childNodes[1];
this.knob=_13.childNodes[2];
this.input=_13.childNodes[3];
}
},postCreate:function(){
this.connect(this.switchNode,"onclick","_onClick");
this.connect(this.switchNode,"onkeydown","_onClick");
this._startHandle=this.connect(this.switchNode,_a.press,"onTouchStart");
this._initialValue=this.value;
},_changeState:function(_14,_15){
var on=(_14==="on");
this.left.style.display="";
this.right.style.display="";
this.inner.style.left="";
if(_15){
_6.add(this.switchNode,"mblSwitchAnimation");
}
_6.remove(this.switchNode,on?"mblSwitchOff":"mblSwitchOn");
_6.add(this.switchNode,on?"mblSwitchOn":"mblSwitchOff");
_9.set(this.switchNode,"aria-checked",on?"true":"false");
var _16=this;
_16.defer(function(){
_16.left.style.display=on?"":"none";
_16.right.style.display=!on?"":"none";
_6.remove(_16.switchNode,"mblSwitchAnimation");
},_15?300:0);
},_createMaskImage:function(){
if(this._timer){
this._timer.remove();
delete this._timer;
}
if(this._hasMaskImage){
return;
}
this._width=this.switchNode.offsetWidth-this.knob.offsetWidth;
this._hasMaskImage=true;
if(!(_d("webkit")||_d("svg"))){
return;
}
var _17=_8.get(this.left,"borderTopLeftRadius");
if(_17=="0px"){
return;
}
var _18=_17.split(" ");
var rx=parseFloat(_18[0]),ry=(_18.length==1)?rx:parseFloat(_18[1]);
var w=this.switchNode.offsetWidth,h=this.switchNode.offsetHeight;
var id=(this.shape+"Mask"+w+h+rx+ry).replace(/\./,"_");
_e.createRoundMask(this.switchNode,0,0,0,0,w,h,rx,ry,1);
},_onClick:function(e){
if(e&&e.type==="keydown"&&e.keyCode!==13){
return;
}
if(this.onClick(e)===false){
return;
}
if(this._moved){
return;
}
this._set("value",this.input.value=(this.value=="on")?"off":"on");
this._changeState(this.value,true);
this.onStateChanged(this.value);
},onClick:function(){
},onTouchStart:function(e){
this._moved=false;
this.innerStartX=this.inner.offsetLeft;
if(!this._conn){
this._conn=[this.connect(this.inner,_a.move,"onTouchMove"),this.connect(_5.doc,_a.release,"onTouchEnd")];
if(_d("windows-theme")){
this._conn.push(this.connect(_5.doc,"MSPointerCancel","onTouchEnd"));
}
}
this.touchStartX=e.touches?e.touches[0].pageX:e.clientX;
this.left.style.display="";
this.right.style.display="";
_4.stop(e);
this._createMaskImage();
},onTouchMove:function(e){
e.preventDefault();
var dx;
if(e.targetTouches){
if(e.targetTouches.length!=1){
return;
}
dx=e.targetTouches[0].clientX-this.touchStartX;
}else{
dx=e.clientX-this.touchStartX;
}
var pos=this.innerStartX+dx;
var d=10;
if(pos<=-(this._width-d)){
pos=-this._width;
}
if(pos>=-d){
pos=0;
}
this.inner.style.left=pos+"px";
if(Math.abs(dx)>d){
this._moved=true;
}
},onTouchEnd:function(e){
_1.forEach(this._conn,_2.disconnect);
this._conn=null;
if(this.innerStartX==this.inner.offsetLeft){
if(_d("touch")&&_d("clicks-prevented")){
dm._sendClick(this.inner,e);
}
return;
}
var _19=(this.inner.offsetLeft<-(this._width/2))?"off":"on";
_19=this._newState(_19);
this._changeState(_19,true);
if(_19!=this.value){
this._set("value",this.input.value=_19);
this.onStateChanged(_19);
}
},_newState:function(_1a){
return _1a;
},onStateChanged:function(_1b){
if(this.labelNode){
this.labelNode.innerHTML=_1b=="off"?this.rightLabel:this.leftLabel;
}
},_setValueAttr:function(_1c){
this._changeState(_1c,false);
if(this.value!=_1c){
this._set("value",this.input.value=_1c);
this.onStateChanged(_1c);
}
},_setLeftLabelAttr:function(_1d){
this.leftLabel=_1d;
this.left.firstChild.innerHTML=this._cv?this._cv(_1d):_1d;
},_setRightLabelAttr:function(_1e){
this.rightLabel=_1e;
this.right.firstChild.innerHTML=this._cv?this._cv(_1e):_1e;
},reset:function(){
this.set("value",this._initialValue);
}});
return _d("dojo-bidi")?_3("dojox.mobile.Switch",[_10,_f]):_10;
});
