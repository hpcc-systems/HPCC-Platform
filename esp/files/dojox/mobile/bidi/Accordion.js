//>>built
define("dojox/mobile/bidi/Accordion",["dojo/_base/declare","./common"],function(_1,_2){
return _1(null,{_setupChild:function(_3){
if(this.textDir){
_3.label=_2.enforceTextDirWithUcc(_3.label,this.textDir);
}
this.inherited(arguments);
}});
});
