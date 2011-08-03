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

