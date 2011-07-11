/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
if (d1 != d1a OR d1 != x'123456', FAIL('d1 did not match'));
output(d1a);

data d2 := x'123456' : stored('d2'); 
data d2a := d2 : stored ('d2a'); 
if (d2 != d2a OR d2 != x'123456', FAIL('d2 did not match'));
output(d2a);