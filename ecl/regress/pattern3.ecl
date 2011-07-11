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

pattern ab := 'a' or 'b';
pattern abstar := ab*;
pattern grammar := abstar 'aba';







infile := DATASET(
    [{'abababcd', 'ababa'},         // two matches
     {'ababababcd', 'ababa'},       // 3 matches
     {'ababababcd', 'ababa'},       // 3 matches
     {'gavin aba halliday aaaba'},
     {'aba'}
     ], { string line, string expected := ''});

results := 
    record
        MATCHTEXT(grammar);
        MATCHTEXT(abstar);
        infile.expected;
    end;


outfile := PARSE(infile,line,grammar,results,SCAN,ALL);

output(outfile);
