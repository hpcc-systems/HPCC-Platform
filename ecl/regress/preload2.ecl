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

export xxNameLib := SERVICE
  data      FirstNameToToken(const string20 name) : c, pure, entrypoint='nameFirstNameToToken';
  string20  TokenToFirstName(const data0 name) : c, pure, entrypoint='nameTokenToFirstName';
  unsigned4 TokenToLength(const data name) : c, pure, entrypoint='nameTokenToLength';
END;

export fnstring20 := TYPE
  export unsigned1 physicallength(const data s) := xxNameLib.TokenToLength(s);
  export string20 load(const data s) := xxNameLib.TokenToFirstName(s);
  export data store(string20 s) := xxNameLib.FirstNameToToken(s);
END;


rec := record
unsigned6    did;
packed integer4  score;
string       unkeyable;
qstring      title{maxlength(4)};
fnstring20   fname;
        end;


set of unsigned6 didSeek := ALL : stored('didSeek');
set of qstring5 titleSeek := ALL : stored('titleSeek');
set of string20 fnameSeek := ALL : stored('fnameSeek');

rawfile := dataset('~thor::rawfile', rec, THOR, preload);

// Combine matches
filtered := rawfile(
 keyed(did in didSeek),
 keyed(title in titleSeek),
 keyed(fname in fnameSeek));

output(filtered)
