//>>built
define("dojox/mobile/_maskUtils",["dojo/_base/window","dojo/dom-style","./sniff"],function(_1,_2,_3){
var _4={};
return {createRoundMask:function(_5,x,y,r,b,w,h,rx,ry,e){
var tw=x+w+r;
var th=y+h+b;
if(_3("webkit")){
var id=("DojoMobileMask"+x+y+w+h+rx+ry).replace(/\./g,"_");
if(!_4[id]){
_4[id]=1;
var _6=_1.doc.getCSSCanvasContext("2d",id,tw,th);
_6.beginPath();
if(rx==ry){
if(rx==2&&w==5){
_6.fillStyle="rgba(0,0,0,0.5)";
_6.fillRect(1,0,3,2);
_6.fillRect(0,1,5,1);
_6.fillRect(0,h-2,5,1);
_6.fillRect(1,h-1,3,2);
_6.fillStyle="rgb(0,0,0)";
_6.fillRect(0,2,5,h-4);
}else{
if(rx==2&&h==5){
_6.fillStyle="rgba(0,0,0,0.5)";
_6.fillRect(0,1,2,3);
_6.fillRect(1,0,1,5);
_6.fillRect(w-2,0,1,5);
_6.fillRect(w-1,1,2,3);
_6.fillStyle="rgb(0,0,0)";
_6.fillRect(2,0,w-4,5);
}else{
_6.fillStyle="#000000";
_6.moveTo(x+rx,y);
_6.arcTo(x,y,x,y+rx,rx);
_6.lineTo(x,y+h-rx);
_6.arcTo(x,y+h,x+rx,y+h,rx);
_6.lineTo(x+w-rx,y+h);
_6.arcTo(x+w,y+h,x+w,y+rx,rx);
_6.lineTo(x+w,y+rx);
_6.arcTo(x+w,y,x+w-rx,y,rx);
}
}
}else{
var pi=Math.PI;
_6.scale(1,ry/rx);
_6.moveTo(x+rx,y);
_6.arc(x+rx,y+rx,rx,1.5*pi,0.5*pi,true);
_6.lineTo(x+w-rx,y+2*rx);
_6.arc(x+w-rx,y+rx,rx,0.5*pi,1.5*pi,true);
}
_6.closePath();
_6.fill();
}
_5.style.webkitMaskImage="-webkit-canvas("+id+")";
}else{
if(_3("svg")){
if(_5._svgMask){
_5.removeChild(_5._svgMask);
}
var bg=null;
for(var p=_5.parentNode;p;p=p.parentNode){
bg=_2.getComputedStyle(p).backgroundColor;
if(bg&&bg!="transparent"&&!bg.match(/rgba\(.*,\s*0\s*\)/)){
break;
}
}
var _7="http://www.w3.org/2000/svg";
var _8=_1.doc.createElementNS(_7,"svg");
_8.setAttribute("width",tw);
_8.setAttribute("height",th);
_8.style.position="absolute";
_8.style.pointerEvents="none";
_8.style.opacity="1";
_8.style.zIndex="2147483647";
var _9=_1.doc.createElementNS(_7,"path");
e=e||0;
rx+=e;
ry+=e;
var d=" M"+(x+rx-e)+","+(y-e)+" a"+rx+","+ry+" 0 0,0 "+(-rx)+","+ry+" v"+(-ry)+" h"+rx+" Z"+" M"+(x-e)+","+(y+h-ry+e)+" a"+rx+","+ry+" 0 0,0 "+rx+","+ry+" h"+(-rx)+" v"+(-ry)+" z"+" M"+(x+w-rx+e)+","+(y+h+e)+" a"+rx+","+ry+" 0 0,0 "+rx+","+(-ry)+" v"+ry+" h"+(-rx)+" z"+" M"+(x+w+e)+","+(y+ry-e)+" a"+rx+","+ry+" 0 0,0 "+(-rx)+","+(-ry)+" h"+rx+" v"+ry+" z";
if(y>0){
d+=" M0,0 h"+tw+" v"+y+" h"+(-tw)+" z";
}
if(b>0){
d+=" M0,"+(y+h)+" h"+tw+" v"+b+" h"+(-tw)+" z";
}
_9.setAttribute("d",d);
_9.setAttribute("fill",bg);
_9.setAttribute("stroke",bg);
_9.style.opacity="1";
_8.appendChild(_9);
_5._svgMask=_8;
_5.appendChild(_8);
}
}
}};
});
