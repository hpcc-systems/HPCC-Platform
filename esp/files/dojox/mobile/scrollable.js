//>>built
define("dojox/mobile/scrollable",["dojo/_base/kernel","dojo/_base/connect","dojo/_base/event","dojo/_base/lang","dojo/_base/window","dojo/dom-class","dojo/dom-construct","dojo/dom-style","dojo/dom-geometry","dojo/touch","./sniff","./_css3","./_maskUtils"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9,_a,_b,_c,_d){
var dm=_4.getObject("dojox.mobile",true);
_b.add("translate3d",function(){
if(_b("css3-animations")){
var _e=_5.doc.createElement("div");
_e.style[_c.name("transform")]="translate3d(0px,1px,0px)";
_5.doc.documentElement.appendChild(_e);
var v=_5.doc.defaultView.getComputedStyle(_e,"")[_c.name("transform",true)];
var _f=v&&v.indexOf("matrix")===0;
_5.doc.documentElement.removeChild(_e);
return _f;
}
});
var _10=function(){
};
_4.extend(_10,{fixedHeaderHeight:0,fixedFooterHeight:0,isLocalFooter:false,scrollBar:true,scrollDir:"v",weight:0.6,fadeScrollBar:true,disableFlashScrollBar:false,threshold:4,constraint:true,touchNode:null,propagatable:true,dirLock:false,height:"",scrollType:0,_parentPadBorderExtentsBottom:0,init:function(_11){
if(_11){
for(var p in _11){
if(_11.hasOwnProperty(p)){
this[p]=((p=="domNode"||p=="containerNode")&&typeof _11[p]=="string")?_5.doc.getElementById(_11[p]):_11[p];
}
}
}
if(typeof this.domNode.style.msTouchAction!="undefined"){
this.domNode.style.msTouchAction="none";
}
this.touchNode=this.touchNode||this.containerNode;
this._v=(this.scrollDir.indexOf("v")!=-1);
this._h=(this.scrollDir.indexOf("h")!=-1);
this._f=(this.scrollDir=="f");
this._ch=[];
this._ch.push(_2.connect(this.touchNode,_a.press,this,"onTouchStart"));
if(_b("css3-animations")){
this._useTopLeft=this.scrollType?this.scrollType===2:_b("android")<3;
if(!this._useTopLeft){
this._useTransformTransition=this.scrollType?this.scrollType===3:_b("ios")>=6;
}
if(!this._useTopLeft){
if(this._useTransformTransition){
this._ch.push(_2.connect(this.domNode,_c.name("transitionEnd"),this,"onFlickAnimationEnd"));
this._ch.push(_2.connect(this.domNode,_c.name("transitionStart"),this,"onFlickAnimationStart"));
}else{
this._ch.push(_2.connect(this.domNode,_c.name("animationEnd"),this,"onFlickAnimationEnd"));
this._ch.push(_2.connect(this.domNode,_c.name("animationStart"),this,"onFlickAnimationStart"));
for(var i=0;i<3;i++){
this.setKeyframes(null,null,i);
}
}
if(_b("translate3d")){
_8.set(this.containerNode,_c.name("transform"),"translate3d(0,0,0)");
}
}else{
this._ch.push(_2.connect(this.domNode,_c.name("transitionEnd"),this,"onFlickAnimationEnd"));
this._ch.push(_2.connect(this.domNode,_c.name("transitionStart"),this,"onFlickAnimationStart"));
}
}
this._speed={x:0,y:0};
this._appFooterHeight=0;
if(this.isTopLevel()&&!this.noResize){
this.resize();
}
var _12=this;
setTimeout(function(){
_12.flashScrollBar();
},600);
if(_5.global.addEventListener){
this._onScroll=function(e){
if(!_12.domNode||_12.domNode.style.display==="none"){
return;
}
var _13=_12.domNode.scrollTop;
var _14=_12.domNode.scrollLeft;
var pos;
if(_13>0||_14>0){
pos=_12.getPos();
_12.domNode.scrollLeft=0;
_12.domNode.scrollTop=0;
_12.scrollTo({x:pos.x-_14,y:pos.y-_13});
}
};
_5.global.addEventListener("scroll",this._onScroll,true);
}
if(!this.disableTouchScroll&&this.domNode.addEventListener){
this._onFocusScroll=function(e){
if(!_12.domNode||_12.domNode.style.display==="none"){
return;
}
var _15=_5.doc.activeElement;
var _16,_17;
if(_15){
_16=_15.getBoundingClientRect();
_17=_12.domNode.getBoundingClientRect();
if(_16.height<_12.getDim().d.h){
if(_16.top<(_17.top+_12.fixedHeaderHeight)){
_12.scrollIntoView(_15,true);
}else{
if((_16.top+_16.height)>(_17.top+_17.height-_12.fixedFooterHeight)){
_12.scrollIntoView(_15,false);
}
}
}
}
};
this.domNode.addEventListener("focus",this._onFocusScroll,true);
}
},isTopLevel:function(){
return true;
},cleanup:function(){
if(this._ch){
for(var i=0;i<this._ch.length;i++){
_2.disconnect(this._ch[i]);
}
this._ch=null;
}
if(this._onScroll&&_5.global.removeEventListener){
_5.global.removeEventListener("scroll",this._onScroll,true);
this._onScroll=null;
}
if(this._onFocusScroll&&this.domNode.removeEventListener){
this.domNode.removeEventListener("focus",this._onFocusScroll,true);
this._onFocusScroll=null;
}
},findDisp:function(_18){
if(!_18.parentNode){
return null;
}
if(_18.nodeType===1&&_6.contains(_18,"mblSwapView")&&_18.style.display!=="none"){
return _18;
}
var _19=_18.parentNode.childNodes;
for(var i=0;i<_19.length;i++){
var n=_19[i];
if(n.nodeType===1&&_6.contains(n,"mblView")&&n.style.display!=="none"){
return n;
}
}
return _18;
},getScreenSize:function(){
return {h:_5.global.innerHeight||_5.doc.documentElement.clientHeight||_5.doc.documentElement.offsetHeight,w:_5.global.innerWidth||_5.doc.documentElement.clientWidth||_5.doc.documentElement.offsetWidth};
},resize:function(e){
this._appFooterHeight=(this._fixedAppFooter)?this._fixedAppFooter.offsetHeight:0;
if(this.isLocalHeader){
this.containerNode.style.marginTop=this.fixedHeaderHeight+"px";
}
var top=0;
for(var n=this.domNode;n&&n.tagName!="BODY";n=n.offsetParent){
n=this.findDisp(n);
if(!n){
break;
}
top+=n.offsetTop+_9.getBorderExtents(n).h;
}
var h,_1a=this.getScreenSize().h,dh=_1a-top-this._appFooterHeight;
if(this.height==="inherit"){
if(this.domNode.offsetParent){
h=_9.getContentBox(this.domNode.offsetParent).h-_9.getBorderExtents(this.domNode).h+"px";
}
}else{
if(this.height==="auto"){
var _1b=this.domNode.offsetParent;
if(_1b){
this.domNode.style.height="0px";
var _1c=_1b.getBoundingClientRect(),_1d=this.domNode.getBoundingClientRect(),_1e=_1c.bottom-this._appFooterHeight-this._parentPadBorderExtentsBottom;
if(_1d.bottom>=_1e){
dh=_1a-(_1d.top-_1c.top)-this._appFooterHeight-this._parentPadBorderExtentsBottom;
}else{
dh=_1e-_1d.bottom;
}
}
var _1f=Math.max(this.domNode.scrollHeight,this.containerNode.scrollHeight);
h=(_1f?Math.min(_1f,dh):dh)+"px";
}else{
if(this.height){
h=this.height;
}
}
}
if(!h){
h=dh+"px";
}
if(h.charAt(0)!=="-"&&h!=="default"){
this.domNode.style.height=h;
}
if(!this._conn){
this.onTouchEnd();
}
},onFlickAnimationStart:function(e){
_3.stop(e);
},onFlickAnimationEnd:function(e){
if(_b("ios")){
this._keepInputCaretInActiveElement();
}
if(e){
var an=e.animationName;
if(an&&an.indexOf("scrollableViewScroll2")===-1){
if(an.indexOf("scrollableViewScroll0")!==-1){
if(this._scrollBarNodeV){
_6.remove(this._scrollBarNodeV,"mblScrollableScrollTo0");
}
}else{
if(an.indexOf("scrollableViewScroll1")!==-1){
if(this._scrollBarNodeH){
_6.remove(this._scrollBarNodeH,"mblScrollableScrollTo1");
}
}else{
if(this._scrollBarNodeV){
this._scrollBarNodeV.className="";
}
if(this._scrollBarNodeH){
this._scrollBarNodeH.className="";
}
}
}
return;
}
if(this._useTransformTransition||this._useTopLeft){
var n=e.target;
if(n===this._scrollBarV||n===this._scrollBarH){
var cls="mblScrollableScrollTo"+(n===this._scrollBarV?"0":"1");
if(_6.contains(n,cls)){
_6.remove(n,cls);
}else{
n.className="";
}
return;
}
}
if(e.srcElement){
_3.stop(e);
}
}
this.stopAnimation();
if(this._bounce){
var _20=this;
var _21=_20._bounce;
setTimeout(function(){
_20.slideTo(_21,0.3,"ease-out");
},0);
_20._bounce=undefined;
}else{
this.hideScrollBar();
this.removeCover();
}
},isFormElement:function(_22){
if(_22&&_22.nodeType!==1){
_22=_22.parentNode;
}
if(!_22||_22.nodeType!==1){
return false;
}
var t=_22.tagName;
return (t==="SELECT"||t==="INPUT"||t==="TEXTAREA"||t==="BUTTON");
},onTouchStart:function(e){
if(this.disableTouchScroll){
return;
}
if(this._conn&&(new Date()).getTime()-this.startTime<500){
return;
}
if(!this._conn){
this._conn=[];
this._conn.push(_2.connect(_5.doc,_a.move,this,"onTouchMove"));
this._conn.push(_2.connect(_5.doc,_a.release,this,"onTouchEnd"));
}
this._aborted=false;
if(_6.contains(this.containerNode,"mblScrollableScrollTo2")){
this.abort();
}else{
if(this._scrollBarNodeV){
this._scrollBarNodeV.className="";
}
if(this._scrollBarNodeH){
this._scrollBarNodeH.className="";
}
}
this.touchStartX=e.touches?e.touches[0].pageX:e.clientX;
this.touchStartY=e.touches?e.touches[0].pageY:e.clientY;
this.startTime=(new Date()).getTime();
this.startPos=this.getPos();
this._dim=this.getDim();
this._time=[0];
this._posX=[this.touchStartX];
this._posY=[this.touchStartY];
this._locked=false;
if(!this.isFormElement(e.target)){
this.propagatable?e.preventDefault():_3.stop(e);
}
},onTouchMove:function(e){
if(this._locked){
return;
}
var x=e.touches?e.touches[0].pageX:e.clientX;
var y=e.touches?e.touches[0].pageY:e.clientY;
var dx=x-this.touchStartX;
var dy=y-this.touchStartY;
var to={x:this.startPos.x+dx,y:this.startPos.y+dy};
var dim=this._dim;
dx=Math.abs(dx);
dy=Math.abs(dy);
if(this._time.length==1){
if(this.dirLock){
if(this._v&&!this._h&&dx>=this.threshold&&dx>=dy||(this._h||this._f)&&!this._v&&dy>=this.threshold&&dy>=dx){
this._locked=true;
return;
}
}
if(this._v&&this._h){
if(dy<this.threshold&&dx<this.threshold){
return;
}
}else{
if(this._v&&dy<this.threshold||(this._h||this._f)&&dx<this.threshold){
return;
}
}
this.addCover();
this.showScrollBar();
}
var _23=this.weight;
if(this._v&&this.constraint){
if(to.y>0){
to.y=Math.round(to.y*_23);
}else{
if(to.y<-dim.o.h){
if(dim.c.h<dim.d.h){
to.y=Math.round(to.y*_23);
}else{
to.y=-dim.o.h-Math.round((-dim.o.h-to.y)*_23);
}
}
}
}
if((this._h||this._f)&&this.constraint){
if(to.x>0){
to.x=Math.round(to.x*_23);
}else{
if(to.x<-dim.o.w){
if(dim.c.w<dim.d.w){
to.x=Math.round(to.x*_23);
}else{
to.x=-dim.o.w-Math.round((-dim.o.w-to.x)*_23);
}
}
}
}
this.scrollTo(to);
var max=10;
var n=this._time.length;
if(n>=2){
var d0,d1;
if(this._v&&!this._h){
d0=this._posY[n-1]-this._posY[n-2];
d1=y-this._posY[n-1];
}else{
if(!this._v&&this._h){
d0=this._posX[n-1]-this._posX[n-2];
d1=x-this._posX[n-1];
}
}
if(d0*d1<0){
this._time=[this._time[n-1]];
this._posX=[this._posX[n-1]];
this._posY=[this._posY[n-1]];
n=1;
}
}
if(n==max){
this._time.shift();
this._posX.shift();
this._posY.shift();
}
this._time.push((new Date()).getTime()-this.startTime);
this._posX.push(x);
this._posY.push(y);
},_keepInputCaretInActiveElement:function(){
var _24=_5.doc.activeElement;
var _25;
if(_24&&(_24.tagName=="INPUT"||_24.tagName=="TEXTAREA")){
_25=_24.value;
if(_24.type=="number"||_24.type=="week"){
if(_25){
_24.value=_24.value+1;
}else{
_24.value=(_24.type=="week")?"2013-W10":1;
}
_24.value=_25;
}else{
_24.value=_24.value+" ";
_24.value=_25;
}
}
},_fingerMovedSinceTouchStart:function(){
var n=this._time.length;
if(n<=1||(n==2&&Math.abs(this._posY[1]-this._posY[0])<4&&_b("touch"))){
return false;
}else{
return true;
}
},onTouchEnd:function(e){
if(this._locked){
return;
}
var _26=this._speed={x:0,y:0};
var dim=this._dim;
var pos=this.getPos();
var to={};
if(e){
if(!this._conn){
return;
}
for(var i=0;i<this._conn.length;i++){
_2.disconnect(this._conn[i]);
}
this._conn=null;
var _27=false;
if(!this._aborted&&!this._fingerMovedSinceTouchStart()){
_27=true;
}
if(_27){
this.hideScrollBar();
this.removeCover();
if(_b("touch")&&_b("clicks-prevented")&&!this.isFormElement(e.target)){
var _28=e.target;
if(_28.nodeType!=1){
_28=_28.parentNode;
}
setTimeout(function(){
dm._sendClick(_28,e);
});
}
return;
}
_26=this._speed=this.getSpeed();
}else{
if(pos.x==0&&pos.y==0){
return;
}
dim=this.getDim();
}
if(this._v){
to.y=pos.y+_26.y;
}
if(this._h||this._f){
to.x=pos.x+_26.x;
}
if(this.adjustDestination(to,pos,dim)===false){
return;
}
if(this.constraint){
if(this.scrollDir=="v"&&dim.c.h<dim.d.h){
this.slideTo({y:0},0.3,"ease-out");
return;
}else{
if(this.scrollDir=="h"&&dim.c.w<dim.d.w){
this.slideTo({x:0},0.3,"ease-out");
return;
}else{
if(this._v&&this._h&&dim.c.h<dim.d.h&&dim.c.w<dim.d.w){
this.slideTo({x:0,y:0},0.3,"ease-out");
return;
}
}
}
}
var _29,_2a="ease-out";
var _2b={};
if(this._v&&this.constraint){
if(to.y>0){
if(pos.y>0){
_29=0.3;
to.y=0;
}else{
to.y=Math.min(to.y,20);
_2a="linear";
_2b.y=0;
}
}else{
if(-_26.y>dim.o.h-(-pos.y)){
if(pos.y<-dim.o.h){
_29=0.3;
to.y=dim.c.h<=dim.d.h?0:-dim.o.h;
}else{
to.y=Math.max(to.y,-dim.o.h-20);
_2a="linear";
_2b.y=-dim.o.h;
}
}
}
}
if((this._h||this._f)&&this.constraint){
if(to.x>0){
if(pos.x>0){
_29=0.3;
to.x=0;
}else{
to.x=Math.min(to.x,20);
_2a="linear";
_2b.x=0;
}
}else{
if(-_26.x>dim.o.w-(-pos.x)){
if(pos.x<-dim.o.w){
_29=0.3;
to.x=dim.c.w<=dim.d.w?0:-dim.o.w;
}else{
to.x=Math.max(to.x,-dim.o.w-20);
_2a="linear";
_2b.x=-dim.o.w;
}
}
}
}
this._bounce=(_2b.x!==undefined||_2b.y!==undefined)?_2b:undefined;
if(_29===undefined){
var _2c,_2d;
if(this._v&&this._h){
_2d=Math.sqrt(_26.x*_26.x+_26.y*_26.y);
_2c=Math.sqrt(Math.pow(to.y-pos.y,2)+Math.pow(to.x-pos.x,2));
}else{
if(this._v){
_2d=_26.y;
_2c=to.y-pos.y;
}else{
if(this._h){
_2d=_26.x;
_2c=to.x-pos.x;
}
}
}
if(_2c===0&&!e){
return;
}
_29=_2d!==0?Math.abs(_2c/_2d):0.01;
}
this.slideTo(to,_29,_2a);
},adjustDestination:function(to,pos,dim){
return true;
},abort:function(){
this._aborted=true;
this.scrollTo(this.getPos());
this.stopAnimation();
},_forceRendering:function(elt){
if(_b("android")>=4.1){
var tmp=elt.style.display;
elt.style.display="none";
elt.offsetHeight;
elt.style.display=tmp;
}
},stopAnimation:function(){
this._forceRendering(this.containerNode);
_6.remove(this.containerNode,"mblScrollableScrollTo2");
if(this._scrollBarV){
this._scrollBarV.className="";
this._forceRendering(this._scrollBarV);
}
if(this._scrollBarH){
this._scrollBarH.className="";
this._forceRendering(this._scrollBarH);
}
if(this._useTransformTransition||this._useTopLeft){
this.containerNode.style[_c.name("transition")]="";
if(this._scrollBarV){
this._scrollBarV.style[_c.name("transition")]="";
}
if(this._scrollBarH){
this._scrollBarH.style[_c.name("transition")]="";
}
}
},scrollIntoView:function(_2e,_2f,_30){
if(!this._v){
return;
}
var c=this.containerNode,h=this.getDim().d.h,top=0;
for(var n=_2e;n!==c;n=n.offsetParent){
if(!n||n.tagName==="BODY"){
return;
}
top+=n.offsetTop;
}
var y=_2f?Math.max(h-c.offsetHeight,-top):Math.min(0,h-top-_2e.offsetHeight);
(_30&&typeof _30==="number")?this.slideTo({y:y},_30,"ease-out"):this.scrollTo({y:y});
},getSpeed:function(){
var x=0,y=0,n=this._time.length;
if(n>=2&&(new Date()).getTime()-this.startTime-this._time[n-1]<500){
var dy=this._posY[n-(n>3?2:1)]-this._posY[(n-6)>=0?n-6:0];
var dx=this._posX[n-(n>3?2:1)]-this._posX[(n-6)>=0?n-6:0];
var dt=this._time[n-(n>3?2:1)]-this._time[(n-6)>=0?n-6:0];
y=this.calcSpeed(dy,dt);
x=this.calcSpeed(dx,dt);
}
return {x:x,y:y};
},calcSpeed:function(_31,_32){
return Math.round(_31/_32*100)*4;
},scrollTo:function(to,_33,_34){
var _35,_36,_37;
var _38=true;
if(!this._aborted&&this._conn){
if(!this._dim){
this._dim=this.getDim();
}
_36=(to.y>0)?to.y:0;
_37=(this._dim.o.h+to.y<0)?-1*(this._dim.o.h+to.y):0;
_35={bubbles:false,cancelable:false,x:to.x,y:to.y,beforeTop:_36>0,beforeTopHeight:_36,afterBottom:_37>0,afterBottomHeight:_37};
_38=this.onBeforeScroll(_35);
}
if(_38){
var s=(_34||this.containerNode).style;
if(_b("css3-animations")){
if(!this._useTopLeft){
if(this._useTransformTransition){
s[_c.name("transition")]="";
}
s[_c.name("transform")]=this.makeTranslateStr(to);
}else{
s[_c.name("transition")]="";
if(this._v){
s.top=to.y+"px";
}
if(this._h||this._f){
s.left=to.x+"px";
}
}
}else{
if(this._v){
s.top=to.y+"px";
}
if(this._h||this._f){
s.left=to.x+"px";
}
}
if(_b("ios")){
this._keepInputCaretInActiveElement();
}
if(!_33){
this.scrollScrollBarTo(this.calcScrollBarPos(to));
}
if(_35){
this.onAfterScroll(_35);
}
}
},onBeforeScroll:function(e){
return true;
},onAfterScroll:function(e){
},slideTo:function(to,_39,_3a){
this._runSlideAnimation(this.getPos(),to,_39,_3a,this.containerNode,2);
this.slideScrollBarTo(to,_39,_3a);
},makeTranslateStr:function(to){
var y=this._v&&typeof to.y=="number"?to.y+"px":"0px";
var x=(this._h||this._f)&&typeof to.x=="number"?to.x+"px":"0px";
return _b("translate3d")?"translate3d("+x+","+y+",0px)":"translate("+x+","+y+")";
},getPos:function(){
if(_b("css3-animations")){
var s=_5.doc.defaultView.getComputedStyle(this.containerNode,"");
if(!this._useTopLeft){
var m=s[_c.name("transform")];
if(m&&m.indexOf("matrix")===0){
var arr=m.split(/[,\s\)]+/);
var i=m.indexOf("matrix3d")===0?12:4;
return {y:arr[i+1]-0,x:arr[i]-0};
}
return {x:0,y:0};
}else{
return {x:parseInt(s.left)||0,y:parseInt(s.top)||0};
}
}else{
var y=parseInt(this.containerNode.style.top)||0;
return {y:y,x:this.containerNode.offsetLeft};
}
},getDim:function(){
var d={};
d.c={h:this.containerNode.offsetHeight,w:this.containerNode.offsetWidth};
d.v={h:this.domNode.offsetHeight+this._appFooterHeight,w:this.domNode.offsetWidth};
d.d={h:d.v.h-this.fixedHeaderHeight-this.fixedFooterHeight-this._appFooterHeight,w:d.v.w};
d.o={h:d.c.h-d.v.h+this.fixedHeaderHeight+this.fixedFooterHeight+this._appFooterHeight,w:d.c.w-d.v.w};
return d;
},showScrollBar:function(){
if(!this.scrollBar){
return;
}
var dim=this._dim;
if(this.scrollDir=="v"&&dim.c.h<=dim.d.h){
return;
}
if(this.scrollDir=="h"&&dim.c.w<=dim.d.w){
return;
}
if(this._v&&this._h&&dim.c.h<=dim.d.h&&dim.c.w<=dim.d.w){
return;
}
var _3b=function(_3c,dir){
var bar=_3c["_scrollBarNode"+dir];
if(!bar){
var _3d=_7.create("div",null,_3c.domNode);
var _3e={position:"absolute",overflow:"hidden"};
if(dir=="V"){
_3e.right="2px";
_3e.width="5px";
}else{
_3e.bottom=(_3c.isLocalFooter?_3c.fixedFooterHeight:0)+2+"px";
_3e.height="5px";
}
_8.set(_3d,_3e);
_3d.className="mblScrollBarWrapper";
_3c["_scrollBarWrapper"+dir]=_3d;
bar=_7.create("div",null,_3d);
_8.set(bar,_c.add({opacity:0.6,position:"absolute",backgroundColor:"#606060",fontSize:"1px",MozBorderRadius:"2px",zIndex:2147483647},{borderRadius:"2px",transformOrigin:"0 0"}));
_8.set(bar,dir=="V"?{width:"5px"}:{height:"5px"});
_3c["_scrollBarNode"+dir]=bar;
}
return bar;
};
if(this._v&&!this._scrollBarV){
this._scrollBarV=_3b(this,"V");
}
if(this._h&&!this._scrollBarH){
this._scrollBarH=_3b(this,"H");
}
this.resetScrollBar();
},hideScrollBar:function(){
if(this.fadeScrollBar&&_b("css3-animations")){
if(!dm._fadeRule){
var _3f=_7.create("style",null,_5.doc.getElementsByTagName("head")[0]);
_3f.textContent=".mblScrollableFadeScrollBar{"+"  "+_c.name("animation-duration",true)+": 1s;"+"  "+_c.name("animation-name",true)+": scrollableViewFadeScrollBar;}"+"@"+_c.name("keyframes",true)+" scrollableViewFadeScrollBar{"+"  from { opacity: 0.6; }"+"  to { opacity: 0; }}";
dm._fadeRule=_3f.sheet.cssRules[1];
}
}
if(!this.scrollBar){
return;
}
var f=function(bar,_40){
_8.set(bar,_c.add({opacity:0},{animationDuration:""}));
if(!(_40._useTopLeft&&_b("android"))){
bar.className="mblScrollableFadeScrollBar";
}
};
if(this._scrollBarV){
f(this._scrollBarV,this);
this._scrollBarV=null;
}
if(this._scrollBarH){
f(this._scrollBarH,this);
this._scrollBarH=null;
}
},calcScrollBarPos:function(to){
var pos={};
var dim=this._dim;
var f=function(_41,_42,t,d,c){
var y=Math.round((d-_42-8)/(d-c)*t);
if(y<-_42+5){
y=-_42+5;
}
if(y>_41-5){
y=_41-5;
}
return y;
};
if(typeof to.y=="number"&&this._scrollBarV){
pos.y=f(this._scrollBarWrapperV.offsetHeight,this._scrollBarV.offsetHeight,to.y,dim.d.h,dim.c.h);
}
if(typeof to.x=="number"&&this._scrollBarH){
pos.x=f(this._scrollBarWrapperH.offsetWidth,this._scrollBarH.offsetWidth,to.x,dim.d.w,dim.c.w);
}
return pos;
},scrollScrollBarTo:function(to){
if(!this.scrollBar){
return;
}
if(this._v&&this._scrollBarV&&typeof to.y=="number"){
if(_b("css3-animations")){
if(!this._useTopLeft){
if(this._useTransformTransition){
this._scrollBarV.style[_c.name("transition")]="";
}
this._scrollBarV.style[_c.name("transform")]=this.makeTranslateStr({y:to.y});
}else{
_8.set(this._scrollBarV,_c.add({top:to.y+"px"},{transition:""}));
}
}else{
this._scrollBarV.style.top=to.y+"px";
}
}
if(this._h&&this._scrollBarH&&typeof to.x=="number"){
if(_b("css3-animations")){
if(!this._useTopLeft){
if(this._useTransformTransition){
this._scrollBarH.style[_c.name("transition")]="";
}
this._scrollBarH.style[_c.name("transform")]=this.makeTranslateStr({x:to.x});
}else{
_8.set(this._scrollBarH,_c.add({left:to.x+"px"},{transition:""}));
}
}else{
this._scrollBarH.style.left=to.x+"px";
}
}
},slideScrollBarTo:function(to,_43,_44){
if(!this.scrollBar){
return;
}
var _45=this.calcScrollBarPos(this.getPos());
var _46=this.calcScrollBarPos(to);
if(this._v&&this._scrollBarV){
this._runSlideAnimation({y:_45.y},{y:_46.y},_43,_44,this._scrollBarV,0);
}
if(this._h&&this._scrollBarH){
this._runSlideAnimation({x:_45.x},{x:_46.x},_43,_44,this._scrollBarH,1);
}
},_runSlideAnimation:function(_47,to,_48,_49,_4a,idx){
if(_b("css3-animations")){
if(!this._useTopLeft){
if(this._useTransformTransition){
if(to.x===undefined){
to.x=_47.x;
}
if(to.y===undefined){
to.y=_47.y;
}
if(to.x!==_47.x||to.y!==_47.y){
_8.set(_4a,_c.add({},{transitionProperty:_c.name("transform"),transitionDuration:_48+"s",transitionTimingFunction:_49}));
var t=this.makeTranslateStr(to);
setTimeout(function(){
_8.set(_4a,_c.add({},{transform:t}));
},0);
_6.add(_4a,"mblScrollableScrollTo"+idx);
}else{
this.hideScrollBar();
this.removeCover();
}
}else{
this.setKeyframes(_47,to,idx);
_8.set(_4a,_c.add({},{animationDuration:_48+"s",animationTimingFunction:_49}));
_6.add(_4a,"mblScrollableScrollTo"+idx);
if(idx==2){
this.scrollTo(to,true,_4a);
}else{
this.scrollScrollBarTo(to);
}
}
}else{
_8.set(_4a,_c.add({},{transitionProperty:"top, left",transitionDuration:_48+"s",transitionTimingFunction:_49}));
setTimeout(function(){
_8.set(_4a,{top:(to.y||0)+"px",left:(to.x||0)+"px"});
},0);
_6.add(_4a,"mblScrollableScrollTo"+idx);
}
}else{
if(_1.fx&&_1.fx.easing&&_48){
var s=_1.fx.slideTo({node:_4a,duration:_48*1000,left:to.x,top:to.y,easing:(_49=="ease-out")?_1.fx.easing.quadOut:_1.fx.easing.linear}).play();
if(idx==2){
_2.connect(s,"onEnd",this,"onFlickAnimationEnd");
}
}else{
if(idx==2){
this.scrollTo(to,false,_4a);
this.onFlickAnimationEnd();
}else{
this.scrollScrollBarTo(to);
}
}
}
},resetScrollBar:function(){
var f=function(_4b,bar,d,c,hd,v){
if(!bar){
return;
}
var _4c={};
_4c[v?"top":"left"]=hd+4+"px";
var t=(d-8)<=0?1:d-8;
_4c[v?"height":"width"]=t+"px";
_8.set(_4b,_4c);
var l=Math.round(d*d/c);
l=Math.min(Math.max(l-8,5),t);
bar.style[v?"height":"width"]=l+"px";
_8.set(bar,{"opacity":0.6});
};
var dim=this.getDim();
f(this._scrollBarWrapperV,this._scrollBarV,dim.d.h,dim.c.h,this.fixedHeaderHeight,true);
f(this._scrollBarWrapperH,this._scrollBarH,dim.d.w,dim.c.w,0);
this.createMask();
},createMask:function(){
if(!(_b("webkit")||_b("svg"))){
return;
}
if(this._scrollBarWrapperV){
var h=this._scrollBarWrapperV.offsetHeight;
_d.createRoundMask(this._scrollBarWrapperV,0,0,0,0,5,h,2,2,0.5);
}
if(this._scrollBarWrapperH){
var w=this._scrollBarWrapperH.offsetWidth;
_d.createRoundMask(this._scrollBarWrapperH,0,0,0,0,w,5,2,2,0.5);
}
},flashScrollBar:function(){
if(this.disableFlashScrollBar||!this.domNode){
return;
}
this._dim=this.getDim();
if(this._dim.d.h<=0){
return;
}
this.showScrollBar();
var _4d=this;
setTimeout(function(){
_4d.hideScrollBar();
},300);
},addCover:function(){
if(!_b("touch")&&!this.noCover){
if(!dm._cover){
dm._cover=_7.create("div",null,_5.doc.body);
dm._cover.className="mblScrollableCover";
_8.set(dm._cover,{backgroundColor:"#ffff00",opacity:0,position:"absolute",top:"0px",left:"0px",width:"100%",height:"100%",zIndex:2147483647});
this._ch.push(_2.connect(dm._cover,_a.press,this,"onTouchEnd"));
}else{
dm._cover.style.display="";
}
this.setSelectable(dm._cover,false);
this.setSelectable(this.domNode,false);
}
},removeCover:function(){
if(!_b("touch")&&dm._cover){
dm._cover.style.display="none";
this.setSelectable(dm._cover,true);
this.setSelectable(this.domNode,true);
}
},setKeyframes:function(_4e,to,idx){
if(!dm._rule){
dm._rule=[];
}
if(!dm._rule[idx]){
var _4f=_7.create("style",null,_5.doc.getElementsByTagName("head")[0]);
_4f.textContent=".mblScrollableScrollTo"+idx+"{"+_c.name("animation-name",true)+": scrollableViewScroll"+idx+";}"+"@"+_c.name("keyframes",true)+" scrollableViewScroll"+idx+"{}";
dm._rule[idx]=_4f.sheet.cssRules[1];
}
var _50=dm._rule[idx];
if(_50){
if(_4e){
_50.deleteRule(_b("webkit")?"from":0);
(_50.insertRule||_50.appendRule).call(_50,"from { "+_c.name("transform",true)+": "+this.makeTranslateStr(_4e)+"; }");
}
if(to){
if(to.x===undefined){
to.x=_4e.x;
}
if(to.y===undefined){
to.y=_4e.y;
}
_50.deleteRule(_b("webkit")?"to":1);
(_50.insertRule||_50.appendRule).call(_50,"to { "+_c.name("transform",true)+": "+this.makeTranslateStr(to)+"; }");
}
}
},setSelectable:function(_51,_52){
_51.style.KhtmlUserSelect=_52?"auto":"none";
_51.style.MozUserSelect=_52?"":"none";
_51.onselectstart=_52?null:function(){
return false;
};
if(_b("ie")){
_51.unselectable=_52?"":"on";
var _53=_51.getElementsByTagName("*");
for(var i=0;i<_53.length;i++){
_53[i].unselectable=_52?"":"on";
}
}
}});
_4.setObject("dojox.mobile.scrollable",_10);
return _10;
});
