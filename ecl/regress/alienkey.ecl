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

export NameLib := SERVICE
  string    FirstNameToToken(const string20 name) : c, pure, entrypoint='nameFirstNameToToken';
  string20  TokenToFirstName(const string token) : c, pure, entrypoint='nameTokenToFirstName';
  unsigned4 TokenToLength(const string name) : c, pure, entrypoint='nameTokenToLength';
END;

export fnstring := TYPE
  export unsigned1 physicallength(const string s) := NameLib.TokenToLength(s);
  export string20 load(const string s) := NameLib.TokenToFirstName(s);
  export string store(string20 s) := NameLib.FirstNameToToken(s);
END;

subRec := record
big_endian unsigned4 id;
packed integer2 score;
  fnstring fname;
        end;

rec := record
unsigned6    did;
subRec       primary;
string       unkeyable;
subRec       secondary;
        end;


unsigned4 searchId := 4 : stored('searchId');
unsigned4 secondaryScore := 3 : stored('secondaryScore');

string20 value := 'JOHN' : stored('value');
set of string20 values := ['JOHN', 'JIM'] : stored('values');

output(dataset([ { 123, { 2, 3, 'JOHN' }, 'unkeyable', {4, 5, 'JULIAN' }}], rec),,'regress::nestfile', OVERWRITE);
rawfile := dataset('regress::nestfile', rec, THOR, preload);

// Combine matches
filtered := rawfile(
 keyed(secondary.id = searchId),
 keyed(primary.score = secondaryScore),
// keyed(primary.fname = value)
 keyed(primary.fname = 'JOHN')
// keyed(primary.fname in ['JOHN         ','a'])
// keyed(primary.fname in values)
);

output(filtered,,'fout', XML, OVERWRITE)

