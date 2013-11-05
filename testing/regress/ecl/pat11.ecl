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


idRec := { unsigned4 id; };


r := record
  string line{maxlength(100)};
  dataset(idRec) ids{maxcount(10)};
end;

ds := dataset([
{'abcabcabcabcabcabcabcabcabcabcabcabcabcabcabc', [{1},{2},{3},{4}]},
{'abcABCabcABCabcabcABCabcABCabcabcABCabcABCabc', [{9},{8}]},
{'AabBcCdDeEfF', [{1}]},
{'', []}],r);

pattern a := 'abc';

pattern b := a nocase(a) a;

pattern S := b;

results := 
    record
        string x := MATCHTEXT;
        string y := MATCHTEXT(a);
        dataset(idRec) ids{maxcount(10)} := ds.ids;
    end;

//Another potential leak - not all results from a parse a read...
output(choosen(PARSE(ds,line,S,results,scan all),2));



