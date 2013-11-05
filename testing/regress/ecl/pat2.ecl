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

pattern a := 'a';
pattern b := 'b';

pattern astar := repeat(a);
pattern aplus := repeat(a, 1, any);
pattern a5 := repeat(a, 5);
pattern a4to6 := repeat(a, 4, 6);


pattern search1 := b astar b;
pattern search2 := b aplus b;
pattern search3 := b a5 b;
pattern search4 := b a4to6 b;
pattern search5 := repeat(any) search2 repeat(any);

infile := dataset([
        {'baabbbbaaaabbaaaaabbaaaaaabbaaaaaaaaaab'},
        {''}
        ], { string text });


output(PARSE(infile,text,search1,{MATCHTEXT(search1)},first,scan)); // should include some nulls.
output(PARSE(infile,text,search1,{MATCHTEXT(search1)},first,scan all));
output(PARSE(infile,text,search2,{MATCHTEXT(search2)},first,scan));
output(PARSE(infile,text,search3,{MATCHTEXT(search3)},first,scan));
output(PARSE(infile,text,search4,{MATCHTEXT(search4)},first,scan));

output(PARSE(infile,text,search5,{MATCHTEXT(search5)},first,noscan));           // should return the first match
output(PARSE(infile,text,search5,{MATCHTEXT(search5)},all,noscan));     // should be same as scan

output(PARSE(infile,text,search2,{MATCHTEXT(search2)},first,scan,keep(2)));
output(PARSE(infile,text,search2,{MATCHTEXT(search2)},first,scan,atmost(4))); // should be blank
output(PARSE(infile,text,search2,{MATCHTEXT(search2)},first,scan,atmost(5)));   // should be ok
