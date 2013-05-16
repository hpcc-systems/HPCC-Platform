//>>built
define("dojox/mobile/scrollable",["dojo/_base/kernel","dojo/_base/connect","dojo/_base/event","dojo/_base/lang","dojo/_base/window","dojo/dom-class","dojo/dom-construct","dojo/dom-style","dojo/touch","./sniff","./_css3","./_maskUtils"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9,_a,_b,_c){
var dm=_4.getObject("dojox.mobile",true);
_a.add("translate3d",function(){
if(_a("css3-animations")){
var _d=_5.doc.createElement("div");
_d.style[_b.name("transform")]="translate3d(0px,1px,0px)";
_5.doc.documentElement.appendChild(_d);
var v=_5.doc.defaultView.getComputedStyle(_d,"")[_b.name("transform",true)];
var _e=v&&v.indexOf("matrix")===0;
_5.doc.documentElement.removeChild(_d);
return _e;
}
});
var _f=function(){
};
_4.extend(_f,{fixedHeaderHeight:0,fixedFooterHeight:0,isLocalFooter:false,scrollBar:true,scrollDir:"v",weight:0.6,fadeScrollBar:true,disableFlashScrollBar:false,threshold:4,constraint:true,touchNode:null,propagatable:true,dirLock:false,height:"",scrollType:0,_parentPadBorderExtentsBottom:0,init:function(_10){
if(_10){
for(var p in _10){
if(_10.hasOwnProperty(p)){
this[p]=((p=="domNode"||p=="containerNode")&&typeof _10[p]=="string")?_5.doc.getElementById(_10[p]):_10[p];
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
this._ch.push(_2.connect(this.touchNode,_9.press,this,"onTouchStart"));
if(_a("css3-animations")){
this._useTopLeft=this.scrollType?this.scrollType===2:_a("android")<3;
if(!this._useTopLeft){
this._useTransformTransition=this.scrollType?this.scrollType===3:_a("ios")>=6;
}
if(!this._useTopLeft){
if(this._useTransformTransition){
this._ch.push(_2.connect(this.domNode,_b.name("transitionEnd"),this,"onFlickAnimationEnd"));
this._ch.push(_2.connect(this.domNode,_b.name("transitionStart"),this,"onFlickAnimationStart"));
}else{
this._ch.push(_2.connect(this.domNode,_b.name("animationEnd"),this,"onFlickAnimationEnd"));
this._ch.push(_2.connect(this.domNode,_b.name("animationStart"),this,"onFlickAnimationStart"));
for(var i=0;i<3;i++){
this.setKeyframes(null,null,i);
}
}
if(_a("translate3d")){
_8.set(this.containerNode,_b.name("transform"),"translate3d(0,0,0)");
}
}else{
this._ch.push(_2.connect(this.domNode,_b.name("transitionEnd"),this,"onFlickAnimationEnd"));
this._ch.push(_2.connect(this.domNode,_b.name("transitionStart"),this,"onFlickAnimationStart"));
}
}
this._speed={x:0,y:0};
this._appFooterHeight=0;
if(this.isTopLevel()&&!this.noResize){
this.resize();
}
var _11=this;
setTimeout(function(){
_11.flashScrollBar();
},600);
if(_5.global.addEventListener){
this._onScroll=function(e){
if(!_11.domNode||_11.domNode.style.display==="none"){
return;
}
var _12=_11.domNode.scrollTop;
var _13=_11.domNode.scrollLeft;
var pos;
if(_12>0||_13>0){
pos=_11.getPos();
_11.domNode.scrollLeft=0;
_11.domNode.scrollTop=0;
_11.scrollTo({x:pos.x-_13,y:pos.y-_12});
}
};
_5.global.addEventListener("scroll",this._onScroll,true);
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
},findDisp:function(_14){
if(!_14.parentNode){
return null;
}
if(_14.nodeType===1&&_6.contains(_14,"mblSwapView")&&_14.style.display!=="none"){
return _14;
}
var _15=_14.parentNode.childNodes;
for(var i=0;i<_15.length;i++){
var n=_15[i];
if(n.nodeType===1&&_6.contains(n,"mblView")&&n.style.display!=="none"){
return n;
}
}
return _14;
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
top+=n.offsetTop;
}
var h,_16=this.getScreenSize().h,dh=_16-top-this._appFooterHeight;
if(this.height==="inherit"){
if(this.domNode.offsetParent){
h=this.domNode.offsetParent.offsetHeight+"px";
}
}else{
if(this.height==="auto"){
var _17=this.domNode.offsetParent;
if(_17){
this.domNode.style.height="0px";
var _18=_17.getBoundingClientRect(),_19=this.domNode.getBoundingClientRect(),_1a=_18.bottom-this._appFooterHeight-this._parentPadBorderExtentsBottom;
if(_19.bottom>=_1a){
dh=_16-(_19.top-_18.top)-this._appFooterHeight-this._parentPadBorderExtentsBottom;
}else{
dh=_1a-_19.bottom;
}
}
var _1b=Math.max(this.domNode.scrollHeight,this.containerNode.scrollHeight);
h=(_1b?Math.min(_1b,dh):dh)+"px";
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
var _1c=this;
var _1d=_1c._bounce;
setTimeout(function(){
_1c.slideTo(_1d,0.3,"ease-out");
},0);
_1c._bounce=undefined;
}else{
this.hideScrollBar();
this.removeCover();
}
},isFormElement:function(_1e){
if(_1e&&_1e.nodeType!==1){
_1e=_1e.parentNode;
}
if(!_1e||_1e.nodeType!==1){
return false;
}
var t=_1e.tagName;
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
this._conn.push(_2.connect(_5.doc,_9.move,this,"onTouchMove"));
this._conn.push(_2.connect(_5.doc,_9.release,this,"onTouchEnd"));
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
var _1f=this.weight;
if(this._v&&this.constraint){
if(to.y>0){
to.y=Math.round(to.y*_1f);
}else{
if(to.y<-dim.o.h){
if(dim.c.h<dim.d.h){
to.y=Math.round(to.y*_1f);
}else{
to.y=-dim.o.h-Math.round((-dim.o.h-to.y)*_1f);
}
}
}
}
if((this._h||this._f)&&this.constraint){
if(to.x>0){
to.x=Math.round(to.x*_1f);
}else{
if(to.x<-dim.o.w){
if(dim.c.w<dim.d.w){
to.x=Math.round(to.x*_1f);
}else{
to.x=-dim.o.w-Math.round((-dim.o.w-to.x)*_1f);
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
},onTouchEnd:function(e){
if(this._locked){
return;
}
var _20=this._speed={x:0,y:0};
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
var n=this._time.length;
var _21=false;
if(!this._aborted){
if(n<=1){
_21=true;
}else{
if(n==2&&Math.abs(this._posY[1]-this._posY[0])<4&&_a("touch")){
_21=true;
}
}
}
if(_21){
this.hideScrollBar();
this.removeCover();
if(_a("touch")&&_a("clicks-prevented")&&!this.isFormElement(e.target)){
var _22=e.target;
if(_22.nodeType!=1){
_22=_22.parentNode;
}
setTimeout(function(){
dm._sendClick(_22,e);
});
}
return;
}
_20=this._speed=this.getSpeed();
}else{
if(pos.x==0&&pos.y==0){
return;
}
dim=this.getDim();
}
if(this._v){
to.y=pos.y+_20.y;
}
if(this._h||this._f){
to.x=pos.x+_20.x;
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
var _23,_24="ease-out";
var _25={};
if(this._v&&this.constraint){
if(to.y>0){
if(pos.y>0){
_23=0.3;
to.y=0;
}else{
to.y=Math.min(to.y,20);
_24="linear";
_25.y=0;
}
}else{
if(-_20.y>dim.o.h-(-pos.y)){
if(pos.y<-dim.o.h){
_23=0.3;
to.y=dim.c.h<=dim.d.h?0:-dim.o.h;
}else{
to.y=Math.max(to.y,-dim.o.h-20);
_24="linear";
_25.y=-dim.o.h;
}
}
}
}
if((this._h||this._f)&&this.constraint){
if(to.x>0){
if(pos.x>0){
_23=0.3;
to.x=0;
}else{
to.x=Math.min(to.x,20);
_24="linear";
_25.x=0;
}
}else{
if(-_20.x>dim.o.w-(-pos.x)){
if(pos.x<-dim.o.w){
_23=0.3;
to.x=dim.c.w<=dim.d.w?0:-dim.o.w;
}else{
to.x=Math.max(to.x,-dim.o.w-20);
_24="linear";
_25.x=-dim.o.w;
}
}
}
}
this._bounce=(_25.x!==undefined||_25.y!==undefined)?_25:undefined;
if(_23===undefined){
var _26,_27;
if(this._v&&this._h){
_27=Math.sqrt(_20.x*_20.x+_20.y*_20.y);
_26=Math.sqrt(Math.pow(to.y-pos.y,2)+Math.pow(to.x-pos.x,2));
}else{
if(this._v){
_27=_20.y;
_26=to.y-pos.y;
}else{
if(this._h){
_27=_20.x;
_26=to.x-pos.x;
}
}
}
if(_26===0&&!e){
return;
}
_23=_27!==0?Math.abs(_26/_27):0.01;
}
this.slideTo(to,_23,_24);
},adjustDestination:function(to,pos,dim){
return true;
},abort:function(){
this._aborted=true;
this.scrollTo(this.getPos());
this.stopAnimation();
},_forceRendering:function(elt){
if(_a("android")>=4.1){
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
this.containerNode.style[_b.name("transition")]="";
if(this._scrollBarV){
this._scrollBarV.style[_b.name("transition")]="";
}
if(this._scrollBarH){
this._scrollBarH.style[_b.name("transition")]="";
}
}
},scrollIntoView:function(_28,_29,_2a){
if(!this._v){
return;
}
var c=this.containerNode,h=this.getDim().d.h,top=0;
for(var n=_28;n!==c;n=n.offsetParent){
if(!n||n.tagName==="BODY"){
return;
}
top+=n.offsetTop;
}
var y=_29?Math.max(h-c.offsetHeight,-top):Math.min(0,h-top-_28.offsetHeight);
(_2a&&typeof _2a==="number")?this.slideTo({y:y},_2a,"ease-out"):this.scrollTo({y:y});
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
},calcSpeed:function(_2b,_2c){
return Math.round(_2b/_2c*100)*4;
},scrollTo:function(to,_2d,_2e){
var _2f,_30,_31;
var _32=true;
if(!this._aborted&&this._conn){
if(!this._dim){
this._dim=this.getDim();
}
_30=(to.y>0)?to.y:0;
_31=(this._dim.o.h+to.y<0)?-1*(this._dim.o.h+to.y):0;
_2f={bubbles:false,cancelable:false,x:to.x,y:to.y,beforeTop:_30>0,beforeTopHeight:_30,afterBottom:_31>0,afterBottomHeight:_31};
_32=this.onBeforeScroll(_2f);
}
if(_32){
var s=(_2e||this.containerNode).style;
if(_a("css3-animations")){
if(!this._useTopLeft){
if(this._useTransformTransition){
s[_b.name("transition")]="";
}
s[_b.name("transform")]=this.makeTranslateStr(to);
}else{
s[_b.name("transition")]="";
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
if(!_2d){
this.scrollScrollBarTo(this.calcScrollBarPos(to));
}
if(_2f){
this.onAfterScroll(_2f);
}
}
},onBeforeScroll:function(e){
return true;
},onAfterScroll:function(e){
},slideTo:function(to,_33,_34){
this._runSlideAnimation(this.getPos(),to,_33,_34,this.containerNode,2);
this.slideScrollBarTo(to,_33,_34);
},makeTranslateStr:function(to){
var y=this._v&&typeof to.y=="number"?to.y+"px":"0px";
var x=(this._h||this._f)&&typeof to.x=="number"?to.x+"px":"0px";
return _a("translate3d")?"translate3d("+x+","+y+",0px)":"translate("+x+","+y+")";
},getPos:function(){
if(_a("css3-animations")){
var s=_5.doc.defaultView.getComputedStyle(this.containerNode,"");
if(!this._useTopLeft){
var m=s[_b.name("transform")];
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
var _35=function(_36,dir){
var bar=_36["_scrollBarNode"+dir];
if(!bar){
var _37=_7.create("div",null,_36.domNode);
var _38={position:"absolute",overflow:"hidden"};
if(dir=="V"){
_38.right="2px";
_38.width="5px";
}else{
_38.bottom=(_36.isLocalFooter?_36.fixedFooterHeight:0)+2+"px";
_38.height="5px";
}
_8.set(_37,_38);
_37.className="mblScrollBarWrapper";
_36["_scrollBarWrapper"+dir]=_37;
bar=_7.create("div",null,_37);
_8.set(bar,_b.add({opacity:0.6,position:"absolute",backgroundColor:"#606060",fontSize:"1px",MozBorderRadius:"2px",zIndex:2147483647},{borderRadius:"2px",transformOrigin:"0 0"}));
_8.set(bar,dir=="V"?{width:"5px"}:{height:"5px"});
_36["_scrollBarNode"+dir]=bar;
}
return bar;
};
if(this._v&&!this._scrollBarV){
this._scrollBarV=_35(this,"V");
}
if(this._h&&!this._scrollBarH){
this._scrollBarH=_35(this,"H");
}
this.resetScrollBar();
},hideScrollBar:function(){
if(this.fadeScrollBar&&_a("css3-animations")){
if(!dm._fadeRule){
var _39=_7.create("style",null,_5.doc.getElementsByTagName("head")[0]);
_39.textContent=".mblScrollableFadeScrollBar{"+"  "+_b.name("animation-duration",true)+": 1s;"+"  "+_b.name("animation-name",true)+": scrollableViewFadeScrollBar;}"+"@"+_b.name("keyframes",true)+" scrollableViewFadeScrollBar{"+"  from { opacity: 0.6; }"+"  to { opacity: 0; }}";
dm._fadeRule=_39.sheet.cssRules[1];
}
}
if(!this.scrollBar){
return;
}
var f=function(bar,_3a){
_8.set(bar,_b.add({opacity:0},{animationDuration:""}));
if(!(_3a._useTopLeft&&_a("android"))){
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
var f=function(_3b,_3c,t,d,c){
var y=Math.round((d-_3c-8)/(d-c)*t);
if(y<-_3c+5){
y=-_3c+5;
}
if(y>_3b-5){
y=_3b-5;
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
if(_a("css3-animations")){
if(!this._useTopLeft){
if(this._useTransformTransition){
this._scrollBarV.style[_b.name("transition")]="";
}
this._scrollBarV.style[_b.name("transform")]=this.makeTranslateStr({y:to.y});
}else{
_8.set(this._scrollBarV,_b.add({top:to.y+"px"},{transition:""}));
}
}else{
this._scrollBarV.style.top=to.y+"px";
}
}
if(this._h&&this._scrollBarH&&typeof to.x=="number"){
if(_a("css3-animations")){
if(!this._useTopLeft){
if(this._useTransformTransition){
this._scrollBarH.style[_b.name("transition")]="";
}
this._scrollBarH.style[_b.name("transform")]=this.makeTranslateStr({x:to.x});
}else{
_8.set(this._scrollBarH,_b.add({left:to.x+"px"},{transition:""}));
}
}else{
this._scrollBarH.style.left=to.x+"px";
}
}
},slideScrollBarTo:function(to,_3d,_3e){
if(!this.scrollBar){
return;
}
var _3f=this.calcScrollBarPos(this.getPos());
var _40=this.calcScrollBarPos(to);
if(this._v&&this._scrollBarV){
this._runSlideAnimation({y:_3f.y},{y:_40.y},_3d,_3e,this._scrollBarV,0);
}
if(this._h&&this._scrollBarH){
this._runSlideAnimation({x:_3f.x},{x:_40.x},_3d,_3e,this._scrollBarH,1);
}
},_runSlideAnimation:function(_41,to,_42,_43,_44,idx){
if(_a("css3-animations")){
if(!this._useTopLeft){
if(this._useTransformTransition){
if(to.x===undefined){
to.x=_41.x;
}
if(to.y===undefined){
to.y=_41.y;
}
if(to.x!==_41.x||to.y!==_41.y){
_8.set(_44,_b.add({},{transitionProperty:_b.name("transform"),transitionDuration:_42+"s",transitionTimingFunction:_43}));
var t=this.makeTranslateStr(to);
setTimeout(function(){
_8.set(_44,_b.add({},{transform:t}));
},0);
_6.add(_44,"mblScrollableScrollTo"+idx);
}else{
this.hideScrollBar();
this.removeCover();
}
}else{
this.setKeyframes(_41,to,idx);
_8.set(_44,_b.add({},{animationDuration:_42+"s",animationTimingFunction:_43}));
_6.add(_44,"mblScrollableScrollTo"+idx);
if(idx==2){
this.scrollTo(to,true,_44);
}else{
this.scrollScrollBarTo(to);
}
}
}else{
_8.set(_44,_b.add({},{transitionProperty:"top, left",transitionDuration:_42+"s",transitionTimingFunction:_43}));
setTimeout(function(){
_8.set(_44,{top:(to.y||0)+"px",left:(to.x||0)+"px"});
},0);
_6.add(_44,"mblScrollableScrollTo"+idx);
}
}else{
if(_1.fx&&_1.fx.easing&&_42){
var s=_1.fx.slideTo({node:_44,duration:_42*1000,left:to.x,top:to.y,easing:(_43=="ease-out")?_1.fx.easing.quadOut:_1.fx.easing.linear}).play();
if(idx==2){
_2.connect(s,"onEnd",this,"onFlickAnimationEnd");
}
}else{
if(idx==2){
this.scrollTo(to,false,_44);
this.onFlickAnimationEnd();
}else{
this.scrollScrollBarTo(to);
}
}
}
},resetScrollBar:function(){
var f=function(_45,bar,d,c,hd,v){
if(!bar){
return;
}
var _46={};
_46[v?"top":"left"]=hd+4+"px";
var t=(d-8)<=0?1:d-8;
_46[v?"height":"width"]=t+"px";
_8.set(_45,_46);
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
if(!(_a("webkit")||_a("svg"))){
return;
}
if(this._scrollBarWrapperV){
var h=this._scrollBarWrapperV.offsetHeight;
_c.createRoundMask(this._scrollBarWrapperV,0,0,0,0,5,h,2,2,0.5);
}
if(this._scrollBarWrapperH){
var w=this._scrollBarWrapperH.offsetWidth;
_c.createRoundMask(this._scrollBarWrapperH,0,0,0,0,w,5,2,2,0.5);
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
var _47=this;
setTimeout(function(){
_47.hideScrollBar();
},300);
},addCover:function(){
if(!_a("touch")&&!this.noCover){
if(!dm._cover){
dm._cover=_7.create("div",null,_5.doc.body);
dm._cover.className="mblScrollableCover";
_8.set(dm._cover,{backgroundColor:"#ffff00",opacity:0,position:"absolute",top:"0px",left:"0px",width:"100%",height:"100%",zIndex:2147483647});
this._ch.push(_2.connect(dm._cover,_9.press,this,"onTouchEnd"));
}else{
dm._cover.style.display="";
}
this.setSelectable(dm._cover,false);
this.setSelectable(this.domNode,false);
}
},removeCover:function(){
if(!_a("touch")&&dm._cover){
dm._cover.style.display="none";
this.setSelectable(dm._cover,true);
this.setSelectable(this.domNode,true);
}
},setKeyframes:function(_48,to,idx){
if(!dm._rule){
dm._rule=[];
}
if(!dm._rule[idx]){
var _49=_7.create("style",null,_5.doc.getElementsByTagName("head")[0]);
_49.textContent=".mblScrollableScrollTo"+idx+"{"+_b.name("animation-name",true)+": scrollableViewScroll"+idx+";}"+"@"+_b.name("keyframes",true)+" scrollableViewScroll"+idx+"{}";
dm._rule[idx]=_49.sheet.cssRules[1];
}
var _4a=dm._rule[idx];
if(_4a){
if(_48){
_4a.deleteRule(_a("webkit")?"from":0);
(_4a.insertRule||_4a.appendRule).call(_4a,"from { "+_b.name("transform",true)+": "+this.makeTranslateStr(_48)+"; }");
}
if(to){
if(to.x===undefined){
to.x=_48.x;
}
if(to.y===undefined){
to.y=_48.y;
}
_4a.deleteRule(_a("webkit")?"to":1);
(_4a.insertRule||_4a.appendRule).call(_4a,"to { "+_b.name("transform",true)+": "+this.makeTranslateStr(to)+"; }");
}
}
},setSelectable:function(_4b,_4c){
_4b.style.KhtmlUserSelect=_4c?"auto":"none";
_4b.style.MozUserSelect=_4c?"":"none";
_4b.onselectstart=_4c?null:function(){
return false;
};
if(_a("ie")){
_4b.unselectable=_4c?"":"on";
var _4d=_4b.getElementsByTagName("*");
for(var i=0;i<_4d.length;i++){
_4d[i].unselectable=_4c?"":"on";
}
}
}});
_4.setObject("dojox.mobile.scrollable",_f);
return _f;
});
