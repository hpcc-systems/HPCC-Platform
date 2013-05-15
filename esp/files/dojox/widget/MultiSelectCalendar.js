//>>built
define("dojox/widget/MultiSelectCalendar",["dojo/main","dijit","dojo/text!./MultiSelectCalendar/MultiSelectCalendar.html","dojo/cldr/supplemental","dojo/date","dojo/date/locale","dijit/_Widget","dijit/_TemplatedMixin","dijit/_WidgetsInTemplateMixin","dijit/_CssStateMixin","dijit/form/DropDownButton","dijit/typematic"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9,_a,_b,_c){
_1.experimental("dojox.widget.MultiSelectCalendar");
var _d=_1.declare("dojox.widget.MultiSelectCalendar",[_7,_8,_9,_a],{templateString:_3,widgetsInTemplate:true,value:{},datePackage:"dojo.date",dayWidth:"narrow",tabIndex:"0",returnIsoRanges:false,currentFocus:new Date(),baseClass:"dijitCalendar",cssStateNodes:{"decrementMonth":"dijitCalendarArrow","incrementMonth":"dijitCalendarArrow","previousYearLabelNode":"dijitCalendarPreviousYear","nextYearLabelNode":"dijitCalendarNextYear"},_areValidDates:function(_e){
for(var _f in this.value){
valid=(_f&&!isNaN(_f)&&typeof _e=="object"&&_f.toString()!=this.constructor.prototype.value.toString());
if(!valid){
return false;
}
}
return true;
},_getValueAttr:function(){
if(this.returnIsoRanges){
datesWithRanges=this._returnDatesWithIsoRanges(this._sort());
return datesWithRanges;
}else{
return this._sort();
}
},_setValueAttr:function(_10,_11){
this.value={};
if(_1.isArray(_10)){
_1.forEach(_10,function(_12,i){
var _13=_12.indexOf("/");
if(_13==-1){
this.value[_12]=1;
}else{
var _14=_1.date.stamp.fromISOString(_12.substr(0,10));
var _15=_1.date.stamp.fromISOString(_12.substr(11,10));
this.toggleDate(_14,[],[]);
if((_14-_15)>0){
this._addToRangeRTL(_14,_15,[],[]);
}else{
this._addToRangeLTR(_14,_15,[],[]);
}
}
},this);
if(_10.length>0){
this.focusOnLastDate(_10[_10.length-1]);
}
}else{
if(_10){
_10=new this.dateClassObj(_10);
}
if(this._isValidDate(_10)){
_10.setHours(1,0,0,0);
if(!this.isDisabledDate(_10,this.lang)){
dateIndex=_1.date.stamp.toISOString(_10).substring(0,10);
this.value[dateIndex]=1;
this.set("currentFocus",_10);
if(_11||typeof _11=="undefined"){
this.onChange(this.get("value"));
this.onValueSelected(this.get("value"));
}
}
}
}
this._populateGrid();
},focusOnLastDate:function(_16){
var _17=_16.indexOf("/");
var _18,_19;
if(_17==-1){
lastDate=_16;
}else{
_18=new _1.date.stamp.fromISOString(_16.substr(0,10));
_19=new _1.date.stamp.fromISOString(_16.substr(11,10));
if((_18-_19)>0){
lastDate=_18;
}else{
lastDate=_19;
}
}
this.set("currentFocus",lastDate);
},_isValidDate:function(_1a){
return _1a&&!isNaN(_1a)&&typeof _1a=="object"&&_1a.toString()!=this.constructor.prototype.value.toString();
},_setText:function(_1b,_1c){
while(_1b.firstChild){
_1b.removeChild(_1b.firstChild);
}
_1b.appendChild(_1.doc.createTextNode(_1c));
},_populateGrid:function(){
var _1d=new this.dateClassObj(this.currentFocus);
_1d.setDate(1);
var _1e=_1d.getDay(),_1f=this.dateFuncObj.getDaysInMonth(_1d),_20=this.dateFuncObj.getDaysInMonth(this.dateFuncObj.add(_1d,"month",-1)),_21=new this.dateClassObj(),_22=_1.cldr.supplemental.getFirstDayOfWeek(this.lang);
if(_22>_1e){
_22-=7;
}
this.listOfNodes=_1.query(".dijitCalendarDateTemplate",this.domNode);
this.listOfNodes.forEach(function(_23,i){
i+=_22;
var _24=new this.dateClassObj(_1d),_25,_26="dijitCalendar",adj=0;
if(i<_1e){
_25=_20-_1e+i+1;
adj=-1;
_26+="Previous";
}else{
if(i>=(_1e+_1f)){
_25=i-_1e-_1f+1;
adj=1;
_26+="Next";
}else{
_25=i-_1e+1;
_26+="Current";
}
}
if(adj){
_24=this.dateFuncObj.add(_24,"month",adj);
}
_24.setDate(_25);
if(!this.dateFuncObj.compare(_24,_21,"date")){
_26="dijitCalendarCurrentDate "+_26;
}
dateIndex=_1.date.stamp.toISOString(_24).substring(0,10);
if(!this.isDisabledDate(_24,this.lang)){
if(this._isSelectedDate(_24,this.lang)){
if(this.value[dateIndex]){
_26="dijitCalendarSelectedDate "+_26;
}else{
_26=_26.replace("dijitCalendarSelectedDate ","");
}
}
}
if(this._isSelectedDate(_24,this.lang)){
_26="dijitCalendarBrowsingDate "+_26;
}
if(this.isDisabledDate(_24,this.lang)){
_26="dijitCalendarDisabledDate "+_26;
}
var _27=this.getClassForDate(_24,this.lang);
if(_27){
_26=_27+" "+_26;
}
_23.className=_26+"Month dijitCalendarDateTemplate";
_23.dijitDateValue=_24.valueOf();
_1.attr(_23,"dijitDateValue",_24.valueOf());
var _28=_1.query(".dijitCalendarDateLabel",_23)[0],_29=_24.getDateLocalized?_24.getDateLocalized(this.lang):_24.getDate();
this._setText(_28,_29);
},this);
var _2a=this.dateLocaleModule.getNames("months","wide","standAlone",this.lang,_1d);
this.monthDropDownButton.dropDown.set("months",_2a);
this.monthDropDownButton.containerNode.innerHTML=(_1.isIE==6?"":"<div class='dijitSpacer'>"+this.monthDropDownButton.dropDown.domNode.innerHTML+"</div>")+"<div class='dijitCalendarMonthLabel dijitCalendarCurrentMonthLabel'>"+_2a[_1d.getMonth()]+"</div>";
var y=_1d.getFullYear()-1;
var d=new this.dateClassObj();
_1.forEach(["previous","current","next"],function(_2b){
d.setFullYear(y++);
this._setText(this[_2b+"YearLabelNode"],this.dateLocaleModule.format(d,{selector:"year",locale:this.lang}));
},this);
},goToToday:function(){
this.set("currentFocus",new this.dateClassObj(),false);
},constructor:function(_2c){
var _2d=(_2c.datePackage&&(_2c.datePackage!="dojo.date"))?_2c.datePackage+".Date":"Date";
this.dateClassObj=_1.getObject(_2d,false);
this.datePackage=_2c.datePackage||this.datePackage;
this.dateFuncObj=_1.getObject(this.datePackage,false);
this.dateLocaleModule=_1.getObject(this.datePackage+".locale",false);
},buildRendering:function(){
this.inherited(arguments);
_1.setSelectable(this.domNode,false);
var _2e=_1.hitch(this,function(_2f,n){
var _30=_1.query(_2f,this.domNode)[0];
for(var i=0;i<n;i++){
_30.parentNode.appendChild(_30.cloneNode(true));
}
});
_2e(".dijitCalendarDayLabelTemplate",6);
_2e(".dijitCalendarDateTemplate",6);
_2e(".dijitCalendarWeekTemplate",5);
var _31=this.dateLocaleModule.getNames("days",this.dayWidth,"standAlone",this.lang);
var _32=_1.cldr.supplemental.getFirstDayOfWeek(this.lang);
_1.query(".dijitCalendarDayLabel",this.domNode).forEach(function(_33,i){
this._setText(_33,_31[(i+_32)%7]);
},this);
var _34=new this.dateClassObj(this.currentFocus);
this.monthDropDownButton.dropDown=new _84({id:this.id+"_mdd",onChange:_1.hitch(this,"_onMonthSelect")});
this.set("currentFocus",_34,false);
var _35=this;
var _36=function(_37,_38,adj){
_35._connects.push(_2.typematic.addMouseListener(_35[_37],_35,function(_39){
if(_39>=0){
_35._adjustDisplay(_38,adj);
}
},0.8,500));
};
_36("incrementMonth","month",1);
_36("decrementMonth","month",-1);
_36("nextYearLabelNode","year",1);
_36("previousYearLabelNode","year",-1);
},_adjustDisplay:function(_3a,_3b){
this._setCurrentFocusAttr(this.dateFuncObj.add(this.currentFocus,_3a,_3b));
},_setCurrentFocusAttr:function(_3c,_3d){
var _3e=this.currentFocus,_3f=_3e?_1.query("[dijitDateValue="+_3e.valueOf()+"]",this.domNode)[0]:null;
_3c=new this.dateClassObj(_3c);
_3c.setHours(1,0,0,0);
this._set("currentFocus",_3c);
var _40=_1.date.stamp.toISOString(_3c).substring(0,7);
if(_40!=this.previousMonth){
this._populateGrid();
this.previousMonth=_40;
}
var _41=_1.query("[dijitDateValue='"+_3c.valueOf()+"']",this.domNode)[0];
_41.setAttribute("tabIndex",this.tabIndex);
if(this._focused||_3d){
_41.focus();
}
if(_3f&&_3f!=_41){
if(_1.isWebKit){
_3f.setAttribute("tabIndex","-1");
}else{
_3f.removeAttribute("tabIndex");
}
}
},focus:function(){
this._setCurrentFocusAttr(this.currentFocus,true);
},_onMonthSelect:function(_42){
this.currentFocus=this.dateFuncObj.add(this.currentFocus,"month",_42-this.currentFocus.getMonth());
this._populateGrid();
},toggleDate:function(_43,_44,_45){
var _46=_1.date.stamp.toISOString(_43).substring(0,10);
if(this.value[_46]){
this.unselectDate(_43,_45);
}else{
this.selectDate(_43,_44);
}
},selectDate:function(_47,_48){
var _49=this._getNodeByDate(_47);
var _4a=_49.className;
var _4b=_1.date.stamp.toISOString(_47).substring(0,10);
this.value[_4b]=1;
_48.push(_4b);
_4a="dijitCalendarSelectedDate "+_4a;
_49.className=_4a;
},unselectDate:function(_4c,_4d){
var _4e=this._getNodeByDate(_4c);
var _4f=_4e.className;
var _50=_1.date.stamp.toISOString(_4c).substring(0,10);
delete (this.value[_50]);
_4d.push(_50);
_4f=_4f.replace("dijitCalendarSelectedDate ","");
_4e.className=_4f;
},_getNodeByDate:function(_51){
var _52=new this.dateClassObj(this.listOfNodes[0].dijitDateValue);
var _53=Math.abs(_1.date.difference(_52,_51,"day"));
return this.listOfNodes[_53];
},_onDayClick:function(evt){
_1.stopEvent(evt);
for(var _54=evt.target;_54&&!_54.dijitDateValue;_54=_54.parentNode){
}
if(_54&&!_1.hasClass(_54,"dijitCalendarDisabledDate")){
value=new this.dateClassObj(_54.dijitDateValue);
if(!this.rangeJustSelected){
this.toggleDate(value,[],[]);
this.previouslySelectedDay=value;
this.set("currentFocus",value);
this.onValueSelected([_1.date.stamp.toISOString(value).substring(0,10)]);
}else{
this.rangeJustSelected=false;
this.set("currentFocus",value);
}
}
},_onDayMouseOver:function(evt){
var _55=_1.hasClass(evt.target,"dijitCalendarDateLabel")?evt.target.parentNode:evt.target;
if(_55&&(_55.dijitDateValue||_55==this.previousYearLabelNode||_55==this.nextYearLabelNode)){
_1.addClass(_55,"dijitCalendarHoveredDate");
this._currentNode=_55;
}
},_setEndRangeAttr:function(_56){
_56=new this.dateClassObj(_56);
_56.setHours(1);
this.endRange=_56;
},_getEndRangeAttr:function(){
var _57=new this.dateClassObj(this.endRange);
_57.setHours(0,0,0,0);
if(_57.getDate()<this.endRange.getDate()){
_57=this.dateFuncObj.add(_57,"hour",1);
}
return _57;
},_onDayMouseOut:function(evt){
if(!this._currentNode){
return;
}
if(evt.relatedTarget&&evt.relatedTarget.parentNode==this._currentNode){
return;
}
var cls="dijitCalendarHoveredDate";
if(_1.hasClass(this._currentNode,"dijitCalendarActiveDate")){
cls+=" dijitCalendarActiveDate";
}
_1.removeClass(this._currentNode,cls);
this._currentNode=null;
},_onDayMouseDown:function(evt){
var _58=evt.target.parentNode;
if(_58&&_58.dijitDateValue){
_1.addClass(_58,"dijitCalendarActiveDate");
this._currentNode=_58;
}
if(evt.shiftKey&&this.previouslySelectedDay){
this.selectingRange=true;
this.set("endRange",_58.dijitDateValue);
this._selectRange();
}else{
this.selectingRange=false;
this.previousRangeStart=null;
this.previousRangeEnd=null;
}
},_onDayMouseUp:function(evt){
var _59=evt.target.parentNode;
if(_59&&_59.dijitDateValue){
_1.removeClass(_59,"dijitCalendarActiveDate");
}
},handleKey:function(evt){
var dk=_1.keys,_5a=-1,_5b,_5c=this.currentFocus;
switch(evt.keyCode){
case dk.RIGHT_ARROW:
_5a=1;
case dk.LEFT_ARROW:
_5b="day";
if(!this.isLeftToRight()){
_5a*=-1;
}
break;
case dk.DOWN_ARROW:
_5a=1;
case dk.UP_ARROW:
_5b="week";
break;
case dk.PAGE_DOWN:
_5a=1;
case dk.PAGE_UP:
_5b=evt.ctrlKey||evt.altKey?"year":"month";
break;
case dk.END:
_5c=this.dateFuncObj.add(_5c,"month",1);
_5b="day";
case dk.HOME:
_5c=new this.dateClassObj(_5c);
_5c.setDate(1);
break;
case dk.ENTER:
case dk.SPACE:
if(evt.shiftKey&&this.previouslySelectedDay){
this.selectingRange=true;
this.set("endRange",_5c);
this._selectRange();
}else{
this.selectingRange=false;
this.toggleDate(_5c,[],[]);
this.previouslySelectedDay=_5c;
this.previousRangeStart=null;
this.previousRangeEnd=null;
this.onValueSelected([_1.date.stamp.toISOString(_5c).substring(0,10)]);
}
break;
default:
return true;
}
if(_5b){
_5c=this.dateFuncObj.add(_5c,_5b,_5a);
}
this.set("currentFocus",_5c);
return false;
},_onKeyDown:function(evt){
if(!this.handleKey(evt)){
_1.stopEvent(evt);
}
},_removeFromRangeLTR:function(_5d,end,_5e,_5f){
difference=Math.abs(_1.date.difference(_5d,end,"day"));
for(var i=0;i<=difference;i++){
var _60=_1.date.add(_5d,"day",i);
this.toggleDate(_60,_5e,_5f);
}
if(this.previousRangeEnd==null){
this.previousRangeEnd=end;
}else{
if(_1.date.compare(end,this.previousRangeEnd,"date")>0){
this.previousRangeEnd=end;
}
}
if(this.previousRangeStart==null){
this.previousRangeStart=end;
}else{
if(_1.date.compare(end,this.previousRangeStart,"date")>0){
this.previousRangeStart=end;
}
}
this.previouslySelectedDay=_1.date.add(_60,"day",1);
},_removeFromRangeRTL:function(_61,end,_62,_63){
difference=Math.abs(_1.date.difference(_61,end,"day"));
for(var i=0;i<=difference;i++){
var _64=_1.date.add(_61,"day",-i);
this.toggleDate(_64,_62,_63);
}
if(this.previousRangeEnd==null){
this.previousRangeEnd=end;
}else{
if(_1.date.compare(end,this.previousRangeEnd,"date")<0){
this.previousRangeEnd=end;
}
}
if(this.previousRangeStart==null){
this.previousRangeStart=end;
}else{
if(_1.date.compare(end,this.previousRangeStart,"date")<0){
this.previousRangeStart=end;
}
}
this.previouslySelectedDay=_1.date.add(_64,"day",-1);
},_addToRangeRTL:function(_65,end,_66,_67){
difference=Math.abs(_1.date.difference(_65,end,"day"));
for(var i=1;i<=difference;i++){
var _68=_1.date.add(_65,"day",-i);
this.toggleDate(_68,_66,_67);
}
if(this.previousRangeStart==null){
this.previousRangeStart=end;
}else{
if(_1.date.compare(end,this.previousRangeStart,"date")<0){
this.previousRangeStart=end;
}
}
if(this.previousRangeEnd==null){
this.previousRangeEnd=_65;
}else{
if(_1.date.compare(_65,this.previousRangeEnd,"date")>0){
this.previousRangeEnd=_65;
}
}
this.previouslySelectedDay=_68;
},_addToRangeLTR:function(_69,end,_6a,_6b){
difference=Math.abs(_1.date.difference(_69,end,"day"));
for(var i=1;i<=difference;i++){
var _6c=_1.date.add(_69,"day",i);
this.toggleDate(_6c,_6a,_6b);
}
if(this.previousRangeStart==null){
this.previousRangeStart=_69;
}else{
if(_1.date.compare(_69,this.previousRangeStart,"date")<0){
this.previousRangeStart=_69;
}
}
if(this.previousRangeEnd==null){
this.previousRangeEnd=end;
}else{
if(_1.date.compare(end,this.previousRangeEnd,"date")>0){
this.previousRangeEnd=end;
}
}
this.previouslySelectedDay=_6c;
},_selectRange:function(){
var _6d=[];
var _6e=[];
var _6f=this.previouslySelectedDay;
var end=this.get("endRange");
if(!this.previousRangeStart&&!this.previousRangeEnd){
removingFromRange=false;
}else{
if((_1.date.compare(end,this.previousRangeStart,"date")<0)||(_1.date.compare(end,this.previousRangeEnd,"date")>0)){
removingFromRange=false;
}else{
removingFromRange=true;
}
}
if(removingFromRange==true){
if(_1.date.compare(end,_6f,"date")<0){
this._removeFromRangeRTL(_6f,end,_6d,_6e);
}else{
this._removeFromRangeLTR(_6f,end,_6d,_6e);
}
}else{
if(_1.date.compare(end,_6f,"date")<0){
this._addToRangeRTL(_6f,end,_6d,_6e);
}else{
this._addToRangeLTR(_6f,end,_6d,_6e);
}
}
if(_6d.length>0){
this.onValueSelected(_6d);
}
if(_6e.length>0){
this.onValueUnselected(_6e);
}
this.rangeJustSelected=true;
},onValueSelected:function(_70){
},onValueUnselected:function(_71){
},onChange:function(_72){
},_isSelectedDate:function(_73,_74){
dateIndex=_1.date.stamp.toISOString(_73).substring(0,10);
return this.value[dateIndex];
},isDisabledDate:function(_75,_76){
},getClassForDate:function(_77,_78){
},_sort:function(){
if(this.value=={}){
return [];
}
var _79=[];
for(var _7a in this.value){
_79.push(_7a);
}
_79.sort(function(a,b){
var _7b=new Date(a),_7c=new Date(b);
return _7b-_7c;
});
return _79;
},_returnDatesWithIsoRanges:function(_7d){
var _7e=[];
if(_7d.length>1){
var _7f=false,_80=0,_81=null,_82=null,_83=_1.date.stamp.fromISOString(_7d[0]);
for(var i=1;i<_7d.length+1;i++){
currentDate=_1.date.stamp.fromISOString(_7d[i]);
if(_7f){
difference=Math.abs(_1.date.difference(_83,currentDate,"day"));
if(difference==1){
_82=currentDate;
}else{
range=_1.date.stamp.toISOString(_81).substring(0,10)+"/"+_1.date.stamp.toISOString(_82).substring(0,10);
_7e.push(range);
_7f=false;
}
}else{
difference=Math.abs(_1.date.difference(_83,currentDate,"day"));
if(difference==1){
_7f=true;
_81=_83;
_82=currentDate;
}else{
_7e.push(_1.date.stamp.toISOString(_83).substring(0,10));
}
}
_83=currentDate;
}
return _7e;
}else{
return _7d;
}
}});
var _84=_d._MonthDropDown=_1.declare("dojox.widget._MonthDropDown",[_7,_8,_9],{months:[],templateString:"<div class='dijitCalendarMonthMenu dijitMenu' "+"dojoAttachEvent='onclick:_onClick,onmouseover:_onMenuHover,onmouseout:_onMenuHover'></div>",_setMonthsAttr:function(_85){
this.domNode.innerHTML=_1.map(_85,function(_86,idx){
return _86?"<div class='dijitCalendarMonthLabel' month='"+idx+"'>"+_86+"</div>":"";
}).join("");
},_onClick:function(evt){
this.onChange(_1.attr(evt.target,"month"));
},onChange:function(_87){
},_onMenuHover:function(evt){
_1.toggleClass(evt.target,"dijitCalendarMonthLabelHover",evt.type=="mouseover");
}});
return _d;
});
require({cache:{"url:dojox/widget/MultiSelectCalendar/MultiSelectCalendar.html":"<table cellspacing=\"0\" cellpadding=\"0\" class=\"dijitCalendarContainer\" role=\"grid\" dojoAttachEvent=\"onkeydown: _onKeyDown\" aria-labelledby=\"${id}_year\">\n\t<thead>\n\t\t<tr class=\"dijitReset dijitCalendarMonthContainer\" valign=\"top\">\n\t\t\t<th class='dijitReset dijitCalendarArrow' dojoAttachPoint=\"decrementMonth\">\n\t\t\t\t<img src=\"${_blankGif}\" alt=\"\" class=\"dijitCalendarIncrementControl dijitCalendarDecrease\" role=\"presentation\"/>\n\t\t\t\t<span dojoAttachPoint=\"decreaseArrowNode\" class=\"dijitA11ySideArrow\">-</span>\n\t\t\t</th>\n\t\t\t<th class='dijitReset' colspan=\"5\">\n\t\t\t\t<div dojoType=\"dijit.form.DropDownButton\" dojoAttachPoint=\"monthDropDownButton\"\n\t\t\t\t\tid=\"${id}_mddb\" tabIndex=\"-1\">\n\t\t\t\t</div>\n\t\t\t</th>\n\t\t\t<th class='dijitReset dijitCalendarArrow' dojoAttachPoint=\"incrementMonth\">\n\t\t\t\t<img src=\"${_blankGif}\" alt=\"\" class=\"dijitCalendarIncrementControl dijitCalendarIncrease\" role=\"presentation\"/>\n\t\t\t\t<span dojoAttachPoint=\"increaseArrowNode\" class=\"dijitA11ySideArrow\">+</span>\n\t\t\t</th>\n\t\t</tr>\n\t\t<tr>\n\t\t\t<th class=\"dijitReset dijitCalendarDayLabelTemplate\" role=\"columnheader\"><span class=\"dijitCalendarDayLabel\"></span></th>\n\t\t</tr>\n\t</thead>\n\t<tbody dojoAttachEvent=\"onclick: _onDayClick, onmouseover: _onDayMouseOver, onmouseout: _onDayMouseOut, onmousedown: _onDayMouseDown, onmouseup: _onDayMouseUp\" class=\"dijitReset dijitCalendarBodyContainer\">\n\t\t<tr class=\"dijitReset dijitCalendarWeekTemplate\" role=\"row\">\n\t\t\t<td class=\"dijitReset dijitCalendarDateTemplate\" role=\"gridcell\"><span class=\"dijitCalendarDateLabel\"></span></td>\n\t\t</tr>\n\t</tbody>\n\t<tfoot class=\"dijitReset dijitCalendarYearContainer\">\n\t\t<tr>\n\t\t\t<td class='dijitReset' valign=\"top\" colspan=\"7\">\n\t\t\t\t<h3 class=\"dijitCalendarYearLabel\">\n\t\t\t\t\t<span dojoAttachPoint=\"previousYearLabelNode\" class=\"dijitInline dijitCalendarPreviousYear\"></span>\n\t\t\t\t\t<span dojoAttachPoint=\"currentYearLabelNode\" class=\"dijitInline dijitCalendarSelectedYear\" id=\"${id}_year\"></span>\n\t\t\t\t\t<span dojoAttachPoint=\"nextYearLabelNode\" class=\"dijitInline dijitCalendarNextYear\"></span>\n\t\t\t\t</h3>\n\t\t\t</td>\n\t\t</tr>\n\t</tfoot>\n</table>"}});
