/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

//sequence should place these storeds in sensible order on the form, in xsd and wsdl
//  field sizes should gradually increase fore each type on form

unsigned2 u2 := 0 : stored('u2', few, format(fieldwidth(2), sequence(12)));
integer2 i2 := 0 : stored('i2', format(fieldwidth(2), sequence(2)));
unsigned7 u7 := 0 : stored('u7', format(fieldwidth(7), sequence(17)));
integer3 i3 := 0 : stored('i3', format(fieldwidth(3), sequence(3)));
integer1 i1 := 0 : stored('i1', format(fieldwidth(1), sequence(1)));
unsigned6 u6 := 0 : stored('u6', format(fieldwidth(6), sequence(16)));
integer6 i6 := 0 : stored('i6', format(fieldwidth(6), sequence(6)));
unsigned1 u1 := 0 : stored('u1', format(fieldwidth(1), sequence(11)));
integer7 i7 := 0 : stored('i7', format(fieldwidth(7), sequence(7)));
integer8 i8 := 0 : stored('i8', format(fieldwidth(8), sequence(8)));
string s1 := 'how now brown cow' : stored('s1', format(fieldwidth(40), fieldheight(10), sequence(20)));
unsigned4 u4 := 0 : stored('u4', format(fieldwidth(4), sequence(14)));
integer5 i5 := 0 : stored('i5', format(fieldwidth(5), sequence(5)));
unsigned5 u5 := 0 : stored('u5', format(fieldwidth(5), sequence(15)));
integer4 i4 := 0 : stored('i4', format(fieldwidth(4), sequence(4)));
unsigned3 u3 := 0 : stored('u3', format(fieldwidth(3), sequence(13)));
unsigned8 u8 := 0 : stored('u8', format(fieldwidth(8), sequence(18), select('one=1,two=2,three=3,*four=4')));

string ch1 := 'ban' : stored('ch1', format(select('apple=app,pear,*banana=ban,orange')));
string ch2 := '' : stored('ch2', format(select(',apple=app,pear,banana=ban,orange'))); //start with empty, no specified default
string ch3 := '' : stored('ch3', format(select('apple=app,pear,*,banana=ban,orange'))); //empty in middle, is default

string pw := 'powow' : stored('pw', format(password, fieldwidth(40)));

output (i1, named('i1'));
output (i2, named('i2'));
output (i3, named('i3'));
output (i4, named('i4'));
output (i5, named('i5'));
output (i6, named('i6'));
output (i7, named('i7'));
output (i8, named('i8'));

output (u1, named('u1'));
output (u2, named('u2'));
output (u3, named('u3'));
output (u4, named('u4'));
output (u5, named('u5'));
output (u6, named('u6'));
output (u7, named('u7'));
output (u8, named('u8'));

output (s1, named('s1'));
output (ch1, named('choice1'));
output (ch2, named('choice2'));
output (ch3, named('choice3'));

output (pw, named('showpw'));
