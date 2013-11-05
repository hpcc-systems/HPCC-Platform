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


r := record
  string line;
  end;

ds := dataset([
{'abcabcabcabcabcabcabcabcabcabcabcabcabcabcabc'},
{'abcABCabcABCabcabcABCabcABCabcabcABCabcABCabc'},
{'AabBcCdDeEfF'},
{''}],r);

pattern a := 'abc';

pattern b := a nocase(a) a;

pattern S := b;

results := 
    record
        string x := MATCHTEXT;
        string y := MATCHTEXT(a);
    end;

output(PARSE(ds,line,S,results,scan all));


pattern b2 := validate(a, matchtext != '');

pattern c2 := b2 nocase(b2) b2;

pattern S2 := c2;

output(PARSE(ds,line,S2,results,scan all));

pattern z := ['a','b','c'] or nocase(['d','e','f']);

output(PARSE(ds,line,z,{MATCHTEXT},scan all));

