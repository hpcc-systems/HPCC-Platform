//>>built
define("dojox/mobile/pageTurningUtils",["dojo/_base/kernel","dojo/_base/array","dojo/_base/connect","dojo/_base/event","dojo/dom-class","dojo/dom-construct","dojo/dom-style","./_css3"],function(_1,_2,_3,_4,_5,_6,_7,_8){
_1.experimental("dojox.mobile.pageTurningUtils");
return function(){
this.w=0;
this.h=0;
this.turnfrom="top";
this.page=1;
this.dogear=1;
this.duration=2;
this.alwaysDogeared=false;
this._styleParams={};
this._catalogNode=null;
this._currentPageNode=null;
this._transitionEndHandle=null;
this.init=function(w,h,_9,_a,_b,_c,_d){
this.w=w;
this.h=h;
this.turnfrom=_9?_9:this.turnfrom;
this.page=_a?_a:this.page;
this.dogear=typeof _b!=="undefined"?_b:this.dogear;
this.duration=typeof _c!=="undefined"?_c:this.duration;
this.alwaysDogeared=typeof _d!=="undefined"?_d:this.alwaysDogeared;
if(this.turnfrom==="bottom"){
this.alwaysDogeared=true;
}
this._calcStyleParams();
};
this._calcStyleParams=function(){
var _e=Math.tan(58*Math.PI/180),_f=Math.cos(32*Math.PI/180),_10=Math.sin(32*Math.PI/180),_11=Math.tan(32*Math.PI/180),w=this.w,h=this.h,_12=this.page,_13=this.turnfrom,_14=this._styleParams;
var Q=fold=w*_e,fw=Q*_10+Q*_f*_e,fh=fold+w+w/_e,dw=w*0.11*this.dogear,pw=w-dw,_15=pw*_f,cx,cy,dx,dy,fy;
switch(this.turnfrom){
case "top":
cx=fw-_15;
cy=_15*_e;
dx=fw-dw;
dy=cy+pw/_e-7;
fy=cy/_f;
_14.init={page:_8.add({top:-fy+"px",left:(-fw+(_12===2?w:0))+"px",width:fw+"px",height:fh+"px"},{transformOrigin:"100% 0%"}),front:_8.add({width:w+"px",height:h+"px"},{boxShadow:"0 0"}),back:_8.add({width:w+"px",height:h+"px"},{boxShadow:"0 0"}),shadow:{display:"",left:fw+"px",height:h*1.5+"px"}};
_14.turnForward={page:_8.add({},{transform:"rotate(0deg)"}),front:_8.add({},{transform:"translate("+fw+"px,"+fy+"px) rotate(0deg)",transformOrigin:"-110px -18px"}),back:_8.add({},{transform:"translate("+(fw-w)+"px,"+fy+"px) rotate(0deg)",transformOrigin:"0px 0px"})};
_14.turnBackward={page:_8.add({},{transform:"rotate(-32deg)"}),front:_8.add({},{transform:"translate("+cx+"px,"+cy+"px) rotate(32deg)",transformOrigin:"0px 0px"}),back:_8.add({},{transform:"translate("+dx+"px,"+dy+"px) rotate(-32deg)",transformOrigin:"0px 0px"})};
break;
case "bottom":
cx=fw-(h*_10+w*_f)-2;
cy=fh-(h+w/_11)*_f;
dx=fw;
dy=fh-w/_10-h;
fy=fh-w/_11-h;
_14.init={page:_8.add({top:(-fy+50)+"px",left:(-fw+(_12===2?w:0))+"px",width:fw+"px",height:fh+"px"},{transformOrigin:"100% 100%"}),front:_8.add({width:w+"px",height:h+"px"},{boxShadow:"0 0"}),back:_8.add({width:w+"px",height:h+"px"},{boxShadow:"0 0"}),shadow:{display:"none"}};
_14.turnForward={page:_8.add({},{transform:"rotate(0deg)"}),front:_8.add({},{transform:"translate("+fw+"px,"+fy+"px) rotate(0deg)",transformOrigin:"-220px 35px"}),back:_8.add({},{transform:"translate("+(w*2)+"px,"+fy+"px) rotate(0deg)",transformOrigin:"0px 0px"})};
_14.turnBackward={page:_8.add({},{transform:"rotate(32deg)"}),front:_8.add({},{transform:"translate("+cx+"px,"+cy+"px) rotate(-32deg)",transformOrigin:"0px 0px"}),back:_8.add({},{transform:"translate("+dx+"px,"+dy+"px) rotate(0deg)",transformOrigin:"0px 0px"})};
break;
case "left":
cx=-w;
cy=pw/_11-2;
dx=-pw;
dy=fy=pw/_10+dw*_10;
_14.init={page:_8.add({top:-cy+"px",left:w+"px",width:fw+"px",height:fh+"px"},{transformOrigin:"0% 0%"}),front:_8.add({width:w+"px",height:h+"px"},{boxShadow:"0 0"}),back:_8.add({width:w+"px",height:h+"px"},{boxShadow:"0 0"}),shadow:{display:"",left:"-4px",height:((_12===2?h*1.5:h)+50)+"px"}};
_14.turnForward={page:_8.add({},{transform:"rotate(0deg)"}),front:_8.add({},{transform:"translate("+cx+"px,"+cy+"px) rotate(0deg)",transformOrigin:"160px 68px"}),back:_8.add({},{transform:"translate(0px,"+cy+"px) rotate(0deg)",transformOrigin:"0px 0px"})};
_14.turnBackward={page:_8.add({},{transform:"rotate(32deg)"}),front:_8.add({},{transform:"translate("+(-dw)+"px,"+dy+"px) rotate(-32deg)",transformOrigin:"0px 0px"}),back:_8.add({},{transform:"translate("+dx+"px,"+dy+"px) rotate(32deg)",transformOrigin:"top right"})};
break;
}
_14.init.catalog={width:(_12===2?w*2:w)+"px",height:((_12===2?h*1.5:h)+(_13=="top"?0:50))+"px"};
};
this.getChildren=function(_16){
return _2.filter(_16.childNodes,function(n){
return n.nodeType===1;
});
};
this.getPages=function(){
return this._catalogNode?this.getChildren(this._catalogNode):null;
};
this.getCurrentPage=function(){
return this._currentPageNode;
};
this.getIndexOfPage=function(_17,_18){
if(!_18){
_18=this.getPages();
}
for(var i=0;i<_18.length;i++){
if(_17===_18[i]){
return i;
}
}
return -1;
};
this.getNextPage=function(_19){
for(var n=_19.nextSibling;n;n=n.nextSibling){
if(n.nodeType===1){
return n;
}
}
return null;
};
this.getPreviousPage=function(_1a){
for(var n=_1a.previousSibling;n;n=n.previousSibling){
if(n.nodeType===1){
return n;
}
}
return null;
};
this.isPageTurned=function(_1b){
return _1b.style[_8.name("transform")]=="rotate(0deg)";
};
this._onPageTurned=function(e){
_4.stop(e);
if(_5.contains(e.target,"mblPageTurningPage")){
this.onPageTurned(e.target);
}
};
this.onPageTurned=function(){
};
this.initCatalog=function(_1c){
if(this._catalogNode!=_1c){
if(this._transitionEndHandle){
_3.disconnect(this._transitionEndHandle);
}
this._transitionEndHandle=_3.connect(_1c,_8.name("transitionEnd"),this,"_onPageTurned");
this._catalogNode=_1c;
}
_5.add(_1c,"mblPageTurningCatalog");
_7.set(_1c,this._styleParams.init.catalog);
var _1d=this.getPages();
_2.forEach(_1d,function(_1e){
this.initPage(_1e);
},this);
this.resetCatalog();
};
this._getBaseZIndex=function(){
return this._catalogNode.style.zIndex||0;
};
this.resetCatalog=function(){
var _1f=this.getPages(),len=_1f.length,_20=this._getBaseZIndex();
for(var i=len-1;i>=0;i--){
var _21=_1f[i];
this.showDogear(_21);
if(this.isPageTurned(_21)){
_21.style.zIndex=_20+len+1;
}else{
_21.style.zIndex=_20+len-i;
!this.alwaysDogeared&&this.hideDogear(_21);
this._currentPageNode=_21;
}
}
if(!this.alwaysDogeared&&this._currentPageNode!=_1f[len-1]){
this.showDogear(this._currentPageNode);
}
};
this.initPage=function(_22,dir){
var _23=this.getChildren(_22);
while(_23.length<3){
_22.appendChild(_6.create("div",null));
_23=this.getChildren(_22);
}
var _24=!_5.contains(_22,"mblPageTurningPage");
_5.add(_22,"mblPageTurningPage");
_5.add(_23[0],"mblPageTurningFront");
_5.add(_23[1],"mblPageTurningBack");
_5.add(_23[2],"mblPageTurningShadow");
var p=this._styleParams.init;
_7.set(_22,p.page);
_7.set(_23[0],p.front);
_7.set(_23[1],p.back);
p.shadow&&_7.set(_23[2],p.shadow);
if(!dir){
if(_24&&this._currentPageNode){
var _25=this.getPages();
dir=this.getIndexOfPage(_22)<this.getIndexOfPage(this._currentPageNode)?1:-1;
}else{
dir=this.isPageTurned(_22)?1:-1;
}
}
this._turnPage(_22,dir,0);
};
this.turnToNext=function(_26){
var _27=this.getNextPage(this._currentPageNode);
if(_27){
this._turnPage(this._currentPageNode,1,_26);
this._currentPageNode=_27;
}
};
this.turnToPrev=function(_28){
var _29=this.getPreviousPage(this._currentPageNode);
if(_29){
this._turnPage(_29,-1,_28);
this._currentPageNode=_29;
}
};
this.goTo=function(_2a){
var _2b=this.getPages();
if(this._currentPageNode===_2b[_2a]||_2b.length<=_2a){
return;
}
var _2c=_2a<this.getIndexOfPage(this._currentPageNode,_2b);
while(this._currentPageNode!==_2b[_2a]){
_2c?this.turnToPrev(0):this.turnToNext(0);
}
};
this._turnPage=function(_2d,dir,_2e){
var _2f=this.getChildren(_2d),d=((typeof _2e!=="undefined")?_2e:this.duration)+"s",p=(dir===1)?this._styleParams.turnForward:this._styleParams.turnBackward;
p.page[_8.name("transitionDuration")]=d;
_7.set(_2d,p.page);
p.front[_8.name("transitionDuration")]=d;
_7.set(_2f[0],p.front);
p.back[_8.name("transitionDuration")]=d;
_7.set(_2f[1],p.back);
var _30=this.getPages(),_31=this.getNextPage(_2d),len=_30.length,_32=this._getBaseZIndex();
if(dir===1){
_2d.style.zIndex=_32+len+1;
if(!this.alwaysDogeared&&_31&&this.getNextPage(_31)){
this.showDogear(_31);
}
}else{
if(_31){
_31.style.zIndex=_32+len-this.getIndexOfPage(_31,_30);
!this.alwaysDogeared&&this.hideDogear(_31);
}
}
};
this.showDogear=function(_33){
var _34=this.getChildren(_33);
_7.set(_33,"overflow","");
_34[1]&&_7.set(_34[1],"display","");
_34[2]&&_7.set(_34[2],"display",this.turnfrom==="bottom"?"none":"");
};
this.hideDogear=function(_35){
if(this.turnfrom==="bottom"){
return;
}
var _36=this.getChildren(_35);
_7.set(_35,"overflow","visible");
_36[1]&&_7.set(_36[1],"display","none");
_36[2]&&_7.set(_36[2],"display","none");
};
};
});
