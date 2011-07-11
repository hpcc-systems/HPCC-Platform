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
