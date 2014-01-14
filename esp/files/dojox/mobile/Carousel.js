//>>built
define("dojox/mobile/Carousel",["dojo/_base/array","dojo/_base/connect","dojo/_base/declare","dojo/_base/event","dojo/_base/lang","dojo/sniff","dojo/dom-class","dojo/dom-construct","dojo/dom-style","dijit/registry","dijit/_Contained","dijit/_Container","dijit/_WidgetBase","./lazyLoadUtils","./CarouselItem","./PageIndicator","./SwapView","require","dojo/has!dojo-bidi?dojox/mobile/bidi/Carousel"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9,_a,_b,_c,_d,_e,_f,_10,_11,_12,_13){
var _14=_3(_6("dojo-bidi")?"dojox.mobile.NonBidiCarousel":"dojox.mobile.Carousel",[_d,_c,_b],{numVisible:2,itemWidth:0,title:"",pageIndicator:true,navButton:false,height:"",selectable:true,baseClass:"mblCarousel",buildRendering:function(){
this.containerNode=_8.create("div",{className:"mblCarouselPages"});
this.inherited(arguments);
var i;
if(this.srcNodeRef){
for(i=0,len=this.srcNodeRef.childNodes.length;i<len;i++){
this.containerNode.appendChild(this.srcNodeRef.firstChild);
}
}
this.headerNode=_8.create("div",{className:"mblCarouselHeaderBar"},this.domNode);
if(this.navButton){
this.btnContainerNode=_8.create("div",{className:"mblCarouselBtnContainer"},this.headerNode);
_9.set(this.btnContainerNode,"float","right");
this.prevBtnNode=_8.create("button",{className:"mblCarouselBtn",title:"Previous",innerHTML:"&lt;"},this.btnContainerNode);
this.nextBtnNode=_8.create("button",{className:"mblCarouselBtn",title:"Next",innerHTML:"&gt;"},this.btnContainerNode);
this._prevHandle=this.connect(this.prevBtnNode,"onclick","onPrevBtnClick");
this._nextHandle=this.connect(this.nextBtnNode,"onclick","onNextBtnClick");
}
if(this.pageIndicator){
if(!this.title){
this.title="&nbsp;";
}
this.piw=new _10();
this.headerNode.appendChild(this.piw.domNode);
}
this.titleNode=_8.create("div",{className:"mblCarouselTitle"},this.headerNode);
this.domNode.appendChild(this.containerNode);
this.subscribe("/dojox/mobile/viewChanged","handleViewChanged");
this.connect(this.domNode,"onclick","_onClick");
this.connect(this.domNode,"onkeydown","_onClick");
this._dragstartHandle=this.connect(this.domNode,"ondragstart",_4.stop);
this.selectedItemIndex=-1;
this.items=[];
},startup:function(){
if(this._started){
return;
}
var h;
if(this.height==="inherit"){
if(this.domNode.offsetParent){
h=this.domNode.offsetParent.offsetHeight+"px";
}
}else{
if(this.height){
h=this.height;
}
}
if(h){
this.domNode.style.height=h;
}
if(this.store){
if(!this.setStore){
throw new Error("Use StoreCarousel or DataCarousel instead of Carousel.");
}
var _15=this.store;
this.store=null;
this.setStore(_15,this.query,this.queryOptions);
}else{
this.resizeItems();
}
this.inherited(arguments);
this.currentView=_1.filter(this.getChildren(),function(_16){
return _16.isVisible();
})[0];
},resizeItems:function(){
var idx=0,i;
var h=this.domNode.offsetHeight-(this.headerNode?this.headerNode.offsetHeight:0);
var m=(_6("ie")<10)?5/this.numVisible-1:5/this.numVisible;
var _17,_18;
_1.forEach(this.getChildren(),function(_19){
if(!(_19 instanceof _11)){
return;
}
if(!(_19.lazy)){
_19._instantiated=true;
}
var ch=_19.containerNode.childNodes;
for(i=0,len=ch.length;i<len;i++){
_17=ch[i];
if(_17.nodeType!==1){
continue;
}
_18=this.items[idx]||{};
_9.set(_17,{width:_18.width||(90/this.numVisible+"%"),height:_18.height||h+"px",margin:"0 "+(_18.margin||m+"%")});
_7.add(_17,"mblCarouselSlot");
idx++;
}
},this);
if(this.piw){
this.piw.refId=this.containerNode.firstChild;
this.piw.reset();
}
},resize:function(){
if(!this.itemWidth){
return;
}
var num=Math.floor(this.domNode.offsetWidth/this.itemWidth);
if(num===this.numVisible){
return;
}
this.selectedItemIndex=this.getIndexByItemWidget(this.selectedItem);
this.numVisible=num;
if(this.items.length>0){
this.onComplete(this.items);
this.select(this.selectedItemIndex);
}
},fillPages:function(){
_1.forEach(this.getChildren(),function(_1a,i){
var s="";
var j;
for(j=0;j<this.numVisible;j++){
var _1b,_1c="",_1d;
var idx=i*this.numVisible+j;
var _1e={};
if(idx<this.items.length){
_1e=this.items[idx];
_1b=this.store.getValue(_1e,"type");
if(_1b){
_1c=this.store.getValue(_1e,"props");
_1d=this.store.getValue(_1e,"mixins");
}else{
_1b="dojox.mobile.CarouselItem";
_1.forEach(["alt","src","headerText","footerText"],function(p){
var v=this.store.getValue(_1e,p);
if(v!==undefined){
if(_1c){
_1c+=",";
}
_1c+=p+":\""+v+"\"";
}
},this);
}
}else{
_1b="dojox.mobile.CarouselItem";
_1c="src:\""+_12.toUrl("dojo/resources/blank.gif")+"\""+", className:\"mblCarouselItemBlank\"";
}
s+="<div data-dojo-type=\""+_1b+"\"";
if(_1c){
s+=" data-dojo-props='"+_1c+"'";
}
if(_1d){
s+=" data-dojo-mixins='"+_1d+"'";
}
s+="></div>";
}
_1a.containerNode.innerHTML=s;
},this);
},onComplete:function(_1f){
_1.forEach(this.getChildren(),function(_20){
if(_20 instanceof _11){
_20.destroyRecursive();
}
});
this.selectedItem=null;
this.items=_1f;
var _21=Math.ceil(_1f.length/this.numVisible),i,h=this.domNode.offsetHeight-this.headerNode.offsetHeight,idx=this.selectedItemIndex===-1?0:this.selectedItemIndex;
pg=Math.floor(idx/this.numVisible);
for(i=0;i<_21;i++){
var w=new _11({height:h+"px",lazy:true});
this.addChild(w);
if(i===pg){
w.show();
this.currentView=w;
}else{
w.hide();
}
}
this.fillPages();
this.resizeItems();
var _22=this.getChildren();
var _23=pg-1<0?0:pg-1;
var to=pg+1>_21-1?_21-1:pg+1;
for(i=_23;i<=to;i++){
this.instantiateView(_22[i]);
}
},onError:function(){
},onUpdate:function(){
},onDelete:function(){
},onSet:function(_24,_25,_26,_27){
},onNew:function(_28,_29){
},onStoreClose:function(_2a){
},getParentView:function(_2b){
var w;
for(w=_a.getEnclosingWidget(_2b);w;w=w.getParent()){
if(w.getParent() instanceof _11){
return w;
}
}
return null;
},getIndexByItemWidget:function(w){
if(!w){
return -1;
}
var _2c=w.getParent();
return _1.indexOf(this.getChildren(),_2c)*this.numVisible+_1.indexOf(_2c.getChildren(),w);
},getItemWidgetByIndex:function(_2d){
if(_2d===-1){
return null;
}
var _2e=this.getChildren()[Math.floor(_2d/this.numVisible)];
return _2e.getChildren()[_2d%this.numVisible];
},onPrevBtnClick:function(){
if(this.currentView){
this.currentView.goTo(-1);
}
},onNextBtnClick:function(){
if(this.currentView){
this.currentView.goTo(1);
}
},_onClick:function(e){
if(this.onClick(e)===false){
return;
}
if(e&&e.type==="keydown"){
if(e.keyCode===39){
this.onNextBtnClick();
}else{
if(e.keyCode===37){
this.onPrevBtnClick();
}else{
if(e.keyCode!==13){
return;
}
}
}
}
var w;
for(w=_a.getEnclosingWidget(e.target);;w=w.getParent()){
if(!w){
return;
}
if(w.getParent() instanceof _11){
break;
}
}
this.select(w);
var idx=this.getIndexByItemWidget(w);
_2.publish("/dojox/mobile/carouselSelect",[this,w,this.items[idx],idx]);
},select:function(_2f){
if(typeof (_2f)==="number"){
_2f=this.getItemWidgetByIndex(_2f);
}
if(this.selectable){
if(this.selectedItem){
this.selectedItem.set("selected",false);
_7.remove(this.selectedItem.domNode,"mblCarouselSlotSelected");
}
if(_2f){
_2f.set("selected",true);
_7.add(_2f.domNode,"mblCarouselSlotSelected");
}
this.selectedItem=_2f;
}
},onClick:function(){
},instantiateView:function(_30){
if(_30&&!_30._instantiated){
var _31=(_9.get(_30.domNode,"display")==="none");
if(_31){
_9.set(_30.domNode,{visibility:"hidden",display:""});
}
_e.instantiateLazyWidgets(_30.containerNode,null,function(_32){
if(_31){
_9.set(_30.domNode,{visibility:"visible",display:"none"});
}
});
_30._instantiated=true;
}
},handleViewChanged:function(_33){
if(_33.getParent()!==this){
return;
}
if(this.currentView.nextView(this.currentView.domNode)===_33){
this.instantiateView(_33.nextView(_33.domNode));
}else{
this.instantiateView(_33.previousView(_33.domNode));
}
this.currentView=_33;
},_setTitleAttr:function(_34){
this.titleNode.innerHTML=this._cv?this._cv(_34):_34;
this._set("title",_34);
}});
_14.ChildSwapViewProperties={lazy:false};
_5.extend(_11,_14.ChildSwapViewProperties);
return _6("dojo-bidi")?_3("dojox.mobile.Carousel",[_14,_13]):_14;
});
