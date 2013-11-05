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

unsigned4 four := 4 : stored('four');
pattern a := 'a';
pattern b := 'b';

token astar := repeat(a);
token aplus := validate(astar, length(matchtext) > 0);
token a5 := validate(astar, length(matchtext) = 5);
token a4to6 := validate(astar, length(matchtext) between four and 6);

token a4to6b := validate(validate(astar, length(matchtext) >=4), length(matchtext) <= 6);


rule search1 := b astar b;
rule search2 := b aplus b;
rule search3 := b a5 b;
rule search4 := b a4to6 b;
rule search4b := b a4to6b b;
rule search5 := repeat(any) search2 repeat(any);

infile := dataset([
        {'baabbbbaaaabbaaaaabbaaaaaabbaaaaaaaaaab'},
        {''}
        ], { string text });


output(PARSE(infile,text,search1,{MATCHTEXT(search1)},first,scan)); // should include some nulls.
output(PARSE(infile,text,search1,{MATCHTEXT(search1)},first,scan all));
output(PARSE(infile,text,search2,{MATCHTEXT(search2)},first,scan));
output(PARSE(infile,text,search3,{MATCHTEXT(search3)},first,scan));
output(PARSE(infile,text,search4,{MATCHTEXT(search4)},first,scan));
output(PARSE(infile,text,search4b,{MATCHTEXT(search4b)},first,scan));

output(PARSE(infile,text,search5,{MATCHTEXT(search5)},first,noscan));           // should return the first match
output(PARSE(infile,text,search5,{MATCHTEXT(search5)},all,noscan));     // should be same as scan

output(PARSE(infile,text,search2,{MATCHTEXT(search2)},first,scan,keep(2)));
output(PARSE(infile,text,search2,{MATCHTEXT(search2)},first,scan,atmost(4))); // should be blank
output(PARSE(infile,text,search2,{MATCHTEXT(search2)},first,scan,atmost(5)));   // should be ok
