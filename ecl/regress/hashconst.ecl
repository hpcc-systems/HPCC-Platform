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



'integers:\n';
output(hash32((unsigned1)10)); '\n';
output(hash32(nofold((unsigned1)10))); '\n';
output(hash32((unsigned2)1010)); '\n';
output(hash32(nofold((unsigned2)1010))); '\n';
output(hash32((unsigned3)1010)); '\n';
output(hash32(nofold((unsigned3)1010))); '\n';
output(hash32((unsigned4)1010)); '\n';
output(hash32(nofold((unsigned4)1010))); '\n';

'reals:\n';
output(hash32((real4)10.10)); '\n';
output(hash32(nofold((real4)10.10))); '\n';
output(hash32((real4)10.50)); '\n';
output(hash32(nofold((real4)10.50))); '\n';
output(hash32((real8)10.10)); '\n';
output(hash32(nofold((real8)10.10))); '\n';

'strings:\n';
output(hash32((string)'abc')); '\n';
output(hash32(nofold((string)'abc'))); '\n';
output(hash32((string)'abc  ')); '\n';
output(hash32(nofold((string)'abc  '))); '\n';

'qstrings:\n';
output(hash32((qstring)'abc')); '\n';
output(hash32(nofold((qstring)'abc'))); '\n';
output(hash32((qstring)'abc  ')); '\n';
output(hash32(nofold((qstring)'abc  '))); '\n';

'varstrings:\n';
output(hash32((varstring)'abc')); '\n';
output(hash32(nofold((varstring)'abc'))); '\n';
output(hash32((varstring)'abc  ')); '\n';
output(hash32(nofold((varstring)'abc  '))); '\n';

'unicode:\n';
output(hash32((unicode)'abc')); '\n';
output(hash32(nofold((unicode)'abc'))); '\n';
output(hash32((unicode)'abc  ')); '\n';
output(hash32(nofold((unicode)'abc  '))); '\n';

'utf8:\n';
output(hash32((utf8)'abc')); '\n';
output(hash32(nofold((utf8)'abc'))); '\n';
output(hash32((utf8)'abc  ')); '\n';
output(hash32(nofold((utf8)'abc  '))); '\n';

'varunicode:\n';
output(hash32((varunicode)'abc')); '\n';
output(hash32(nofold((varunicode)'abc'))); '\n';
output(hash32((varunicode)'abc  ')); '\n';
output(hash32(nofold((varunicode)'abc  '))); '\n';

'combined:\n';
output(hash32('abc  ',99,'def')); '\n';
output(hash32(nofold('abc  '),99,'def')); '\n';
output(hash32('abc',99,'def')); '\n';
output(hash32(nofold('abc  '),99,'def')); '\n';

'integers:\n';
output(hash64((unsigned1)10)); '\n';
output(hash64(nofold((unsigned1)10))); '\n';
output(hash64((unsigned2)1010)); '\n';
output(hash64(nofold((unsigned2)1010))); '\n';
output(hash64((unsigned3)1010)); '\n';
output(hash64(nofold((unsigned3)1010))); '\n';
output(hash64((unsigned4)1010)); '\n';
output(hash64(nofold((unsigned4)1010))); '\n';

'reals:\n';
output(hash64((real4)10.10)); '\n';
output(hash64(nofold((real4)10.10))); '\n';
output(hash64((real4)10.50)); '\n';
output(hash64(nofold((real4)10.50))); '\n';
output(hash64((real8)10.10)); '\n';
output(hash64(nofold((real8)10.10))); '\n';

'strings:\n';
output(hash64((string)'abc')); '\n';
output(hash64(nofold((string)'abc'))); '\n';
output(hash64((string)'abc  ')); '\n';
output(hash64(nofold((string)'abc  '))); '\n';

'qstrings:\n';
output(hash64((qstring)'abc')); '\n';
output(hash64(nofold((qstring)'abc'))); '\n';
output(hash64((qstring)'abc  ')); '\n';
output(hash64(nofold((qstring)'abc  '))); '\n';

'varstrings:\n';
output(hash64((varstring)'abc')); '\n';
output(hash64(nofold((varstring)'abc'))); '\n';
output(hash64((varstring)'abc  ')); '\n';
output(hash64(nofold((varstring)'abc  '))); '\n';

'unicode:\n';
output(hash64((unicode)'abc')); '\n';
output(hash64(nofold((unicode)'abc'))); '\n';
output(hash64((unicode)'abc  ')); '\n';
output(hash64(nofold((unicode)'abc  '))); '\n';

'utf8:\n';
output(hash64((utf8)'abc')); '\n';
output(hash64(nofold((utf8)'abc'))); '\n';
output(hash64((utf8)'abc  ')); '\n';
output(hash64(nofold((utf8)'abc  '))); '\n';

'varunicode:\n';
output(hash64((varunicode)'abc')); '\n';
output(hash64(nofold((varunicode)'abc'))); '\n';
output(hash64((varunicode)'abc  ')); '\n';
output(hash64(nofold((varunicode)'abc  '))); '\n';

'combined:\n';
output(hash64('abc  ',99,'def')); '\n';
output(hash64(nofold('abc  '),99,'def')); '\n';
output(hash64('abc',99,'def')); '\n';
output(hash64(nofold('abc'),99,'def')); '\n';
