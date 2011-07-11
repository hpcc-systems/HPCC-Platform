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
        ], { unicode text });


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
