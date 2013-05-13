//>>built
define("dojox/mobile/common",["dojo/_base/array","dojo/_base/config","dojo/_base/connect","dojo/_base/lang","dojo/_base/window","dojo/_base/kernel","dojo/dom-class","dojo/dom-construct","dojo/ready","dojo/touch","dijit/registry","./sniff","./uacss"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9,_a,_b,_c){
var dm=_4.getObject("dojox.mobile",true);
_5.doc.dojoClick=true;
if(_c("touch")){
_c.add("clicks-prevented",!(_c("android")>=4.1||_c("ie")>=10));
if(_c("clicks-prevented")){
dm._sendClick=function(_d,e){
for(var _e=_d;_e;_e=_e.parentNode){
if(_e.dojoClick){
return;
}
}
var ev=_5.doc.createEvent("MouseEvents");
ev.initMouseEvent("click",true,true,_5.global,1,e.screenX,e.screenY,e.clientX,e.clientY);
_d.dispatchEvent(ev);
};
}
}
dm.getScreenSize=function(){
return {h:_5.global.innerHeight||_5.doc.documentElement.clientHeight,w:_5.global.innerWidth||_5.doc.documentElement.clientWidth};
};
dm.updateOrient=function(){
var _f=dm.getScreenSize();
_7.replace(_5.doc.documentElement,_f.h>_f.w?"dj_portrait":"dj_landscape",_f.h>_f.w?"dj_landscape":"dj_portrait");
};
dm.updateOrient();
dm.tabletSize=500;
dm.detectScreenSize=function(_10){
var dim=dm.getScreenSize();
var sz=Math.min(dim.w,dim.h);
var _11,to;
if(sz>=dm.tabletSize&&(_10||(!this._sz||this._sz<dm.tabletSize))){
_11="phone";
to="tablet";
}else{
if(sz<dm.tabletSize&&(_10||(!this._sz||this._sz>=dm.tabletSize))){
_11="tablet";
to="phone";
}
}
if(to){
_7.replace(_5.doc.documentElement,"dj_"+to,"dj_"+_11);
_3.publish("/dojox/mobile/screenSize/"+to,[dim]);
}
this._sz=sz;
};
dm.detectScreenSize();
dm.hideAddressBarWait=typeof (_2["mblHideAddressBarWait"])==="number"?_2["mblHideAddressBarWait"]:1500;
dm.hide_1=function(){
scrollTo(0,1);
dm._hidingTimer=(dm._hidingTimer==0)?200:dm._hidingTimer*2;
setTimeout(function(){
if(dm.isAddressBarHidden()||dm._hidingTimer>dm.hideAddressBarWait){
dm.resizeAll();
dm._hiding=false;
}else{
setTimeout(dm.hide_1,dm._hidingTimer);
}
},50);
};
dm.hideAddressBar=function(evt){
if(dm.disableHideAddressBar||dm._hiding){
return;
}
dm._hiding=true;
dm._hidingTimer=_c("ios")?200:0;
var _12=screen.availHeight;
if(_c("android")){
_12=outerHeight/devicePixelRatio;
if(_12==0){
dm._hiding=false;
setTimeout(function(){
dm.hideAddressBar();
},200);
}
if(_12<=innerHeight){
_12=outerHeight;
}
if(_c("android")<3){
_5.doc.documentElement.style.overflow=_5.body().style.overflow="visible";
}
}
if(_5.body().offsetHeight<_12){
_5.body().style.minHeight=_12+"px";
dm._resetMinHeight=true;
}
setTimeout(dm.hide_1,dm._hidingTimer);
};
dm.isAddressBarHidden=function(){
return pageYOffset===1;
};
dm.resizeAll=function(evt,_13){
if(dm.disableResizeAll){
return;
}
_3.publish("/dojox/mobile/resizeAll",[evt,_13]);
_3.publish("/dojox/mobile/beforeResizeAll",[evt,_13]);
if(dm._resetMinHeight){
_5.body().style.minHeight=dm.getScreenSize().h+"px";
}
dm.updateOrient();
dm.detectScreenSize();
var _14=function(w){
var _15=w.getParent&&w.getParent();
return !!((!_15||!_15.resize)&&w.resize);
};
var _16=function(w){
_1.forEach(w.getChildren(),function(_17){
if(_14(_17)){
_17.resize();
}
_16(_17);
});
};
if(_13){
if(_13.resize){
_13.resize();
}
_16(_13);
}else{
_1.forEach(_1.filter(_b.toArray(),_14),function(w){
w.resize();
});
}
_3.publish("/dojox/mobile/afterResizeAll",[evt,_13]);
};
dm.openWindow=function(url,_18){
_5.global.open(url,_18||"_blank");
};
dm._detectWindowsTheme=function(){
if(navigator.userAgent.match(/IEMobile\/10\.0/)){
_8.create("style",{innerHTML:"@-ms-viewport {width: auto !important}"},_5.doc.head);
}
var _19=function(){
_7.add(_5.doc.documentElement,"windows_theme");
_6.experimental("Dojo Mobile Windows theme","Behavior and appearance of the Windows theme are experimental.");
};
var _1a=_c("windows-theme");
if(_1a!==undefined){
if(_1a){
_19();
}
return;
}
var i,j;
var _1b=function(_1c){
if(_1c&&_1c.indexOf("/windows/")!==-1){
_c.add("windows-theme",true);
_19();
return true;
}
return false;
};
var s=_5.doc.styleSheets;
for(i=0;i<s.length;i++){
if(s[i].href){
continue;
}
var r=s[i].cssRules||s[i].imports;
if(!r){
continue;
}
for(j=0;j<r.length;j++){
if(_1b(r[j].href)){
return;
}
}
}
var _1d=_5.doc.getElementsByTagName("link");
for(i=0;i<_1d.length;i++){
if(_1b(_1d[i].href)){
return;
}
}
};
if(_2["mblApplyPageStyles"]!==false){
_7.add(_5.doc.documentElement,"mobile");
}
if(_c("chrome")){
_7.add(_5.doc.documentElement,"dj_chrome");
}
if(_5.global._no_dojo_dm){
var _1e=_5.global._no_dojo_dm;
for(var i in _1e){
dm[i]=_1e[i];
}
dm.deviceTheme.setDm(dm);
}
_c.add("mblAndroidWorkaround",_2["mblAndroidWorkaround"]!==false&&_c("android")<3,undefined,true);
_c.add("mblAndroid3Workaround",_2["mblAndroid3Workaround"]!==false&&_c("android")>=3,undefined,true);
dm._detectWindowsTheme();
_9(function(){
dm.detectScreenSize(true);
_7.add(_5.body(),"mblBackground");
if(_2["mblAndroidWorkaroundButtonStyle"]!==false&&_c("android")){
_8.create("style",{innerHTML:"BUTTON,INPUT[type='button'],INPUT[type='submit'],INPUT[type='reset'],INPUT[type='file']::-webkit-file-upload-button{-webkit-appearance:none;} audio::-webkit-media-controls-play-button,video::-webkit-media-controls-play-button{-webkit-appearance:media-play-button;} video::-webkit-media-controls-fullscreen-button{-webkit-appearance:media-fullscreen-button;}"},_5.doc.head,"first");
}
if(_c("mblAndroidWorkaround")){
_8.create("style",{innerHTML:".mblView.mblAndroidWorkaround{position:absolute;top:-9999px !important;left:-9999px !important;}"},_5.doc.head,"last");
}
var f=dm.resizeAll;
if(_2["mblHideAddressBar"]!==false&&navigator.appVersion.indexOf("Mobile")!=-1||_2["mblForceHideAddressBar"]===true){
dm.hideAddressBar();
if(_2["mblAlwaysHideAddressBar"]===true){
f=dm.hideAddressBar;
}
}
var _1f=_c("ios")>=6;
if((_c("android")||_1f)&&_5.global.onorientationchange!==undefined){
var _20=f;
var _21,_22,_23;
if(_1f){
_22=_5.doc.documentElement.clientWidth;
_23=_5.doc.documentElement.clientHeight;
}else{
f=function(evt){
var _24=_3.connect(null,"onresize",null,function(e){
_3.disconnect(_24);
_20(e);
});
};
_21=dm.getScreenSize();
}
_3.connect(null,"onresize",null,function(e){
if(_1f){
var _25=_5.doc.documentElement.clientWidth,_26=_5.doc.documentElement.clientHeight;
if(_25==_22&&_26!=_23){
_20(e);
}
_22=_25;
_23=_26;
}else{
var _27=dm.getScreenSize();
if(_27.w==_21.w&&Math.abs(_27.h-_21.h)>=100){
_20(e);
}
_21=_27;
}
});
}
_3.connect(null,_5.global.onorientationchange!==undefined?"onorientationchange":"onresize",null,f);
_5.body().style.visibility="visible";
});
return dm;
});
