//>>built
define("dojox/charting/plot2d/commonStacked",["dojo/_base/lang","./common"],function(_1,_2){
var _3=_1.getObject("dojox.charting.plot2d.commonStacked",true);
return _1.mixin(_3,{collectStats:function(_4){
var _5=_1.delegate(_2.defaultStats);
for(var i=0;i<_4.length;++i){
var _6=_4[i];
for(var j=0;j<_6.data.length;j++){
var x,y;
if(_6.data[j]!==null){
if(typeof _6.data[j]=="number"||!_6.data[j].hasOwnProperty("x")){
y=_3.getIndexValue(_4,i,j)[0];
x=j+1;
}else{
x=_6.data[j].x;
if(x!==null){
y=_3.getValue(_4,i,x)[0];
y=y!=null&&y.y?y.y:null;
}
}
_5.hmin=Math.min(_5.hmin,x);
_5.hmax=Math.max(_5.hmax,x);
_5.vmin=Math.min(_5.vmin,y);
_5.vmax=Math.max(_5.vmax,y);
}
}
}
return _5;
},getIndexValue:function(_7,i,_8){
var _9=0,v,j,_a;
for(j=0;j<=i;++j){
_a=_9;
v=_7[j].data[_8];
if(v!=null){
if(isNaN(v)){
v=v.y||0;
}
_9+=v;
}
}
return [_9,_a];
},getValue:function(_b,i,x){
var _c=null,j,z,v,_d;
for(j=0;j<=i;++j){
for(z=0;z<_b[j].data.length;z++){
_d=_c;
v=_b[j].data[z];
if(v!==null){
if(v.x==x){
if(!_c){
_c={x:x};
}
if(v.y!=null){
if(_c.y==null){
_c.y=0;
}
_c.y+=v.y;
}
break;
}else{
if(v.x>x){
break;
}
}
}
}
}
return [_c,_d];
}});
});
