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
pattern c := 'c';
pattern d := 'd';

pattern x1 := 'x1';
pattern x3 := 'x3';
pattern x4 := 'x4';

pattern y1 := 'y1';
pattern y2 := 'Z'*;

pattern y := y1 y2;
pattern x2 := y;
pattern x := x1 x2 x3 x4;
pattern grammar := a x b y c x d;








infile := DATASET(
    [{'ax1y1ZZZx3x4by1ZZcx1y1Zx3x4d'},          // two matches
     {'aba'}
     ], { string line, string expected := ''});

results :=
    record
        MATCHTEXT(grammar);
        MATCHTEXT(y2);          // ZZZ
        MATCHTEXT(y2[2]);       // ZZ
        MATCHTEXT(y2[3]);       // Z
    end;


outfile := PARSE(infile,line,grammar,results,SCAN,ALL);

output(outfile);
