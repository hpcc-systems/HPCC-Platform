/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
