//>>built
define("dojox/date/umalqura/Date",["dojo/_base/lang","dojo/_base/declare","dojo/date"],function(_1,_2,dd){
var _3=_2("dojox.date.umalqura.Date",null,{_MONTH_LENGTH:["010100101011","011010010011","010110110101","001010110110","101110110010","011010110101","010101010110","101010010110","110101001010","111010100101","011101010010","101101101001","010101110100","101001101101","100100110110","110010010110","110101001010","111001101001","011010110100","101010111010","010010111101","001000111101","100100011101","101010010101","101101001010","101101011010","010101101101","001010110110","100100111011","010010011011","011001010101","011010101001","011101010100","101101101010","010101101100","101010101101","010101010101","101100101001","101110010010","101110101001","010111010100","101011011010","010101011010","101010101011","010110010101","011101001001","011101100100","101110101010","010110110101","001010110110","101001010110","110100101010","111010010101","011100101010","011101010101","001101011010","100101011101","010010011011","101001001101","110100100110","110101010011","010110101010","101010101101","010010110110","101001010111","010100100111","101010010101","101101001010","101101010101","001101101100","100110101110","010010110110","101010010110","101101001010","110110100101","010111010010","010111011001","001011011100","100101101101","010010101101","011001010101"],_hijriBegin:1400,_hijriEnd:1480,_date:0,_month:0,_year:0,_hours:0,_minutes:0,_seconds:0,_milliseconds:0,_day:0,constructor:function(){
var _4=arguments.length;
if(!_4){
this.fromGregorian(new Date());
}else{
if(_4==1){
var _5=arguments[0];
if(typeof _5=="number"){
_5=new Date(_5);
}
if(_5 instanceof Date){
this.fromGregorian(_5);
}else{
if(_5==""){
this._date=new Date("");
}else{
this._year=_5._year;
this._month=_5._month;
this._date=_5._date;
this._hours=_5._hours;
this._minutes=_5._minutes;
this._seconds=_5._seconds;
this._milliseconds=_5._milliseconds;
}
}
}else{
if(_4>=3){
this._year+=arguments[0];
this._month+=arguments[1];
this._date+=arguments[2];
this._hours+=arguments[3]||0;
this._minutes+=arguments[4]||0;
this._seconds+=arguments[5]||0;
this._milliseconds+=arguments[6]||0;
}
}
}
},getDate:function(){
return this._date;
},getMonth:function(){
return this._month;
},getFullYear:function(){
return this._year;
},getDay:function(){
var d=this.toGregorian();
var dd=d.getDay();
return dd;
},getHours:function(){
return this._hours;
},getMinutes:function(){
return this._minutes;
},getSeconds:function(){
return this._seconds;
},getMilliseconds:function(){
return this._milliseconds;
},setDate:function(_6){
_6=parseInt(_6);
if(_6>0&&_6<=this.getDaysInIslamicMonth(this._month,this._year)){
this._date=_6;
}else{
var _7;
if(_6>0){
for(_7=this.getDaysInIslamicMonth(this._month,this._year);_6>_7;_6-=_7,_7=this.getDaysInIslamicMonth(this._month,this._year)){
this._month++;
if(this._month>=12){
this._year++;
this._month-=12;
}
}
this._date=_6;
}else{
for(_7=this.getDaysInIslamicMonth((this._month-1)>=0?(this._month-1):11,((this._month-1)>=0)?this._year:this._year-1);_6<=0;_7=this.getDaysInIslamicMonth((this._month-1)>=0?(this._month-1):11,((this._month-1)>=0)?this._year:this._year-1)){
this._month--;
if(this._month<0){
this._year--;
this._month+=12;
}
_6+=_7;
}
this._date=_6;
}
}
return this;
},setFullYear:function(_8){
this._year=+_8;
},setMonth:function(_9){
this._year+=Math.floor(_9/12);
if(_9>0){
this._month=Math.floor(_9%12);
}else{
this._month=Math.floor(((_9%12)+12)%12);
}
},setHours:function(){
var _a=arguments.length;
var _b=0;
if(_a>=1){
_b=parseInt(arguments[0]);
}
if(_a>=2){
this._minutes=parseInt(arguments[1]);
}
if(_a>=3){
this._seconds=parseInt(arguments[2]);
}
if(_a==4){
this._milliseconds=parseInt(arguments[3]);
}
while(_b>=24){
this._date++;
var _c=this.getDaysInIslamicMonth(this._month,this._year);
if(this._date>_c){
this._month++;
if(this._month>=12){
this._year++;
this._month-=12;
}
this._date-=_c;
}
_b-=24;
}
this._hours=_b;
},_addMinutes:function(_d){
_d+=this._minutes;
this.setMinutes(_d);
this.setHours(this._hours+parseInt(_d/60));
return this;
},_addSeconds:function(_e){
_e+=this._seconds;
this.setSeconds(_e);
this._addMinutes(parseInt(_e/60));
return this;
},_addMilliseconds:function(_f){
_f+=this._milliseconds;
this.setMilliseconds(_f);
this._addSeconds(parseInt(_f/1000));
return this;
},setMinutes:function(_10){
while(_10>=60){
this._hours++;
if(this._hours>=24){
this._date++;
this._hours-=24;
var _11=this.getDaysInIslamicMonth(this._month,this._year);
if(this._date>_11){
this._month++;
if(this._month>=12){
this._year++;
this._month-=12;
}
this._date-=_11;
}
}
_10-=60;
}
this._minutes=_10;
},setSeconds:function(_12){
while(_12>=60){
this._minutes++;
if(this._minutes>=60){
this._hours++;
this._minutes-=60;
if(this._hours>=24){
this._date++;
this._hours-=24;
var _13=this.getDaysInIslamicMonth(this._month,this._year);
if(this._date>_13){
this._month++;
if(this._month>=12){
this._year++;
this._month-=12;
}
this._date-=_13;
}
}
}
_12-=60;
}
this._seconds=_12;
},setMilliseconds:function(_14){
while(_14>=1000){
this.setSeconds++;
if(this.setSeconds>=60){
this._minutes++;
this.setSeconds-=60;
if(this._minutes>=60){
this._hours++;
this._minutes-=60;
if(this._hours>=24){
this._date++;
this._hours-=24;
var _15=this.getDaysInIslamicMonth(this._month,this._year);
if(this._date>_15){
this._month++;
if(this._month>=12){
this._year++;
this._month-=12;
}
this._date-=_15;
}
}
}
}
_14-=1000;
}
this._milliseconds=_14;
},toString:function(){
var x=new Date();
x.setHours(this._hours);
x.setMinutes(this._minutes);
x.setSeconds(this._seconds);
x.setMilliseconds(this._milliseconds);
return this._month+" "+this._date+" "+this._year+" "+x.toTimeString();
},toGregorian:function(){
var _16=this._year;
var _17=this._month;
var _18=this._date;
var _19=new Date();
if(_16>=this._hijriBegin&&_16<=this._hijriEnd){
var _1a=new Array(17);
_1a[0]=new Date(1979,10,20,0,0,0,0);
_1a[1]=new Date(1984,8,26,0,0,0,0);
_1a[2]=new Date(1989,7,3,0,0,0,0);
_1a[3]=new Date(1994,5,10,0,0,0,0);
_1a[4]=new Date(1999,3,17,0,0,0,0);
_1a[5]=new Date(2004,1,21,0,0,0,0);
_1a[6]=new Date(2008,11,29,0,0,0,0);
_1a[7]=new Date(2013,10,4,0,0,0,0);
_1a[8]=new Date(2018,8,11,0,0,0,0);
_1a[9]=new Date(2023,6,19,0,0,0,0);
_1a[10]=new Date(2028,4,25,0,0,0,0);
_1a[11]=new Date(2033,3,1,0,0,0,0);
_1a[12]=new Date(2038,1,5,0,0,0,0);
_1a[13]=new Date(2042,11,14,0,0,0,0);
_1a[14]=new Date(2047,9,20,0,0,0,0);
_1a[15]=new Date(2052,7,26,0,0,0,0);
_1a[16]=new Date(2057,6,3,0,0,0,0);
var i=(_16-this._hijriBegin);
var a=Math.floor(i/5);
var b=i%5;
var _1b=0;
var m=b;
var _1c=a*5;
var l=0;
var h=0;
if(b==0){
for(h=0;h<=_17-1;h++){
if(this._MONTH_LENGTH[i].charAt(h)=="1"){
_1b=_1b+30;
}else{
if(this._MONTH_LENGTH[i].charAt(h)=="0"){
_1b=_1b+29;
}
}
}
_1b=_1b+(_18-1);
}else{
for(k=_1c;k<=(_1c+b);k++){
for(l=0;m>0&&l<12;l++){
if(this._MONTH_LENGTH[k].charAt(l)=="1"){
_1b=_1b+30;
}else{
if(this._MONTH_LENGTH[k].charAt(l)=="0"){
_1b=_1b+29;
}
}
}
m--;
if(m==0){
for(h=0;h<=_17-1;h++){
if(this._MONTH_LENGTH[i].charAt(h)=="1"){
_1b=_1b+30;
}else{
if(this._MONTH_LENGTH[i].charAt(h)=="0"){
_1b=_1b+29;
}
}
}
}
}
_1b=_1b+(_18-1);
}
var _1d=new Date(_1a[a]);
_1d.setHours(this._hours,this._minutes,this._seconds,this._milliseconds);
_19=dd.add(_1d,"day",_1b);
}else{
var _1e=new dojox.date.islamic.Date(this._year,this._month,this._date,this._hours,this._minutes,this._seconds,this._milliseconds);
_19=new Date(_1e.toGregorian());
}
return _19;
},fromGregorian:function(_1f){
var _20=new Date(_1f);
_20.setHours(0,0,0,0);
var _21=_20.getFullYear(),_22=_20.getMonth(),_23=_20.getDate();
var _24=new Array(17);
_24[0]=new Date(1979,10,20,0,0,0,0);
_24[1]=new Date(1984,8,26,0,0,0,0);
_24[2]=new Date(1989,7,3,0,0,0,0);
_24[3]=new Date(1994,5,10,0,0,0,0);
_24[4]=new Date(1999,3,17,0,0,0,0);
_24[5]=new Date(2004,1,21,0,0,0,0);
_24[6]=new Date(2008,11,29,0,0,0,0);
_24[7]=new Date(2013,10,4,0,0,0,0);
_24[8]=new Date(2018,8,11,0,0,0,0);
_24[9]=new Date(2023,6,19,0,0,0,0);
_24[10]=new Date(2028,4,25,0,0,0,0);
_24[11]=new Date(2033,3,1,0,0,0,0);
_24[12]=new Date(2038,1,5,0,0,0,0);
_24[13]=new Date(2042,11,14,0,0,0,0);
_24[14]=new Date(2047,9,20,0,0,0,0);
_24[15]=new Date(2052,7,26,0,0,0,0);
_24[16]=new Date(2057,6,3,0,0,0,0);
var _25=new Date(2058,5,21,0,0,0,0);
if(dd.compare(_20,_24[0])>=0&&dd.compare(_20,_25)<=0){
var _26;
if(dd.compare(_20,_24[16])<=0){
var _27=0;
var pos=0;
var _28=0;
for(_27=0;_27<_24.length;_27++){
if(dd.compare(_20,_24[_27],"date")==0){
pos=_27;
_28=1;
break;
}else{
if(dd.compare(_20,_24[_27],"date")<0){
pos=_27-1;
break;
}
}
}
var j=0;
var _29=0;
var _2a=0;
if(_28==1){
this._date=1;
this._month=0;
this._year=this._hijriBegin+pos*5;
this._hours=_1f.getHours();
this._minutes=_1f.getMinutes();
this._seconds=_1f.getSeconds();
this._milliseconds=_1f.getMilliseconds();
this._day=_24[pos].getDay();
}else{
_26=dd.difference(_24[pos],_20,"day");
pos=pos*5;
for(i=pos;i<pos+5;i++){
for(j=0;j<=11;j++){
if(this._MONTH_LENGTH[i].charAt(j)=="1"){
_2a=30;
}else{
if(this._MONTH_LENGTH[i].charAt(j)=="0"){
_2a=29;
}
}
if(_26>_2a){
_26=_26-_2a;
}else{
_29=1;
break;
}
}
if(_29==1){
if(_26==0){
_26=1;
if(j==11){
j=1;
++i;
}else{
++j;
}
break;
}else{
if(_26==_2a){
_26=0;
if(j==11){
j=0;
++i;
}else{
++j;
}
}
_26++;
break;
}
}
}
this._date=_26;
this._month=j;
this._year=this._hijriBegin+i;
this._hours=_1f.getHours();
this._minutes=_1f.getMinutes();
this._seconds=_1f.getSeconds();
this._milliseconds=_1f.getMilliseconds();
this._day=_1f.getDay();
}
}else{
_26=dd.difference(_24[16],_20,"day");
var x=dd.difference(new Date(2057,6,3,0,0,0,0),new Date(2057,6,1,0,0,0,0),"date");
for(j=0;j<=11;j++){
if(this._MONTH_LENGTH[80].charAt(j)=="1"){
_2a=30;
}else{
if(this._MONTH_LENGTH[80].charAt(j)=="0"){
_2a=29;
}
}
if(_26>_2a){
_26=_26-_2a;
}else{
_29=1;
break;
}
}
if(_29==1){
if(_26==0){
_26=1;
if(j==11){
j=1;
++i;
}else{
++j;
}
}else{
if(_26==_2a){
_26=0;
if(j==11){
j=0;
++i;
}else{
++j;
}
}
_26++;
}
}
this._date=_26;
this._month=j;
this._year=1480;
this._hours=_1f.getHours();
this._minutes=_1f.getMinutes();
this._seconds=_1f.getSeconds();
this._milliseconds=_1f.getMilliseconds();
this._day=_1f.getDay();
}
}else{
var _2b=new dojox.date.islamic.Date(_20);
this._date=_2b.getDate();
this._month=_2b.getMonth();
this._year=_2b.getFullYear();
this._hours=_1f.getHours();
this._minutes=_1f.getMinutes();
this._seconds=_1f.getSeconds();
this._milliseconds=_1f.getMilliseconds();
this._day=_1f.getDay();
}
return this;
},valueOf:function(){
return (this.toGregorian()).valueOf();
},_yearStart:function(_2c){
return (_2c-1)*354+Math.floor((3+11*_2c)/30);
},_monthStart:function(_2d,_2e){
return Math.ceil(29.5*_2e)+(_2d-1)*354+Math.floor((3+11*_2d)/30);
},_civilLeapYear:function(_2f){
return (14+11*_2f)%30<11;
},getDaysInIslamicMonth:function(_30,_31){
if(_31>=this._hijriBegin&&_31<=this._hijriEnd){
var pos=_31-this._hijriBegin;
var _32=0;
if(this._MONTH_LENGTH[pos].charAt(_30)==1){
_32=30;
}else{
_32=29;
}
}else{
var _33=new dojox.date.islamic.Date();
_32=_33.getDaysInIslamicMonth(_30,_31);
}
return _32;
}});
_3.getDaysInIslamicMonth=function(_34){
return new _3().getDaysInIslamicMonth(_34.getMonth(),_34.getFullYear());
};
return _3;
});
