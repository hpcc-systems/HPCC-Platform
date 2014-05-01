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

//Check consistency of equality and the hash functions.  NOTE: hashcrc and Hashmd5 are not expected to be consistent for ignore characters and trailing spaces

ucompare(unicode a, unicode b) :=
    [ output('Unicode:'); output(a = b); output(hash(a) = hash(b)); output(hash32(a) = hash32(b)); output(hash64(a) = hash64(b)) ];

vcompare(varunicode a, varunicode b) :=
    [ output('VarUnicode:'); output(a = b); output(hash(a) = hash(b)); output(hash32(a) = hash32(b)); output(hash64(a) = hash64(b)) ];

compare8(utf8 a, utf8 b) :=
    [ output('Utf8:'); output(a = b); output(hash(a) = hash(b)); output(hash32(a) = hash32(b)); output(hash64(a) = hash64(b)) ];

compare(unicode a, unicode b) :=
    [ output('Compare: "' + a + '" with "' + b + '"'); ucompare(a,b); vcompare(a, b), compare8(a,b) ];

compare(U'Gavin', U'Gavin');
compare(U'Gavin   ', U'Gavin');

compare(U'Gavin\u00ADHalliday', U'GavinHalliday');
compare(U'Gavin\u200BHalliday', U'GavinHalliday');

compare(U'Gavin\u00AD\u200B\u200B\u200BHalliday', U'GavinHalliday');
compare(U'Gavin\u200BHalliday', U'Gavin\u200B\u200B\u200BHalliday');

hashes(unicode a) :=
    [ output('Hashes: "' + a + '"');
      output(hash(a) = hash((varunicode)a));
      output(hash32(a) = hash32((varunicode)a));
      output(hash64(a) = hash64((varunicode)a));
      output(hash(a) = hash((utf8)a));
      output(hash32(a) = hash32((utf8)a));
      output(hash64(a) = hash64((utf8)a));
      output(hash(a)); output(hash32(a)); output(hash64(a));
      output(hashcrc(a)); output(hashmd5(a)) ];


hashes(U'Gavin');
hashes(U'Gavin   ');
hashes(U'G\u00ADav\u200B\u200Bin   ');
hashes(U'G\U0001D11Eavin   ');		// G clef surrogate pair character
