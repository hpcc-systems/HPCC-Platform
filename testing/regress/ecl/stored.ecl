/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

string20 s1 := '12345' : stored('s1'); 
string20 s1a := s1 : stored ('s1a'); 
if (s1 != s1a OR s1 != '12345', FAIL('s1 did not match'));
output(s1a);

varstring20 vs1 := '12345' : stored('vs1'); 
varstring20 vs1a := vs1 : stored ('vs1a'); 
if (vs1 != vs1a OR vs1 != '12345', FAIL('vs1 did not match'));
output(vs1a);

string s2 := '12345' : stored('s2'); 
string s2a := s2 : stored ('s2a'); 
if (s2 != s2a OR s2 != '12345', FAIL('s2 did not match'));
output(s2a);

varstring vs2 := '12345' : stored('vs2'); 
varstring vs2a := vs2 : stored ('vs2a'); 
if (vs2 != vs2a OR vs2 != '12345', FAIL('vs2 did not match'));
output(vs2a);

unicode20 u1 := u'12345' : stored('u1'); 
unicode20 u1a := u1 : stored ('u1a'); 
if (u1 != u1a OR u1 != u'12345', FAIL('u1 did not match'));
output(u1a);

varunicode20 vu1 := u'12345' : stored('vu1'); 
varunicode20 vu1a := vu1 : stored ('vu1a'); 
if (vu1 != vu1a OR vu1 != u'12345', FAIL('vu1 did not match'));
output(vu1a);

unicode u2 := u'12345' : stored('u2'); 
unicode u2a := u2 : stored ('u2a'); 
if (u2 != u2a OR u2 != u'12345', FAIL('u2 did not match'));
output(u2a);

varunicode vu2 := u'12345' : stored('vu2'); 
varunicode vu2a := vu2 : stored ('vu2a'); 
if (vu2 != vu2a OR vu2 != u'12345', FAIL('vu2 did not match'));
output(vu2a);

data20 d1 := x'123456' : stored('d1'); 
data20 d1a := d1 : stored ('d1a'); 
if (d1 != d1a OR d1 != (data20)x'123456', FAIL('d1 did not match'));
output(d1a);

data d2 := x'123456' : stored('d2'); 
data d2a := d2 : stored ('d2a'); 
if (d2 != d2a OR d2 != x'123456', FAIL('d2 did not match'));
output(d2a);