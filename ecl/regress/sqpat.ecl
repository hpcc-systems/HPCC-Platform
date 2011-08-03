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


pattern ws := [' ','\t'];
pattern patStart := FIRST | ws;
pattern patEnd := LAST | ws;

token patWord := PATTERN('[a-z]+');     // deliberately lower case only
rule sentance := patStart patWord repeat(ws patWord) patEnd;
rule sentance2 := patWord+;

stringRec := { string value; };
inRec := { string line, dataset(stringRec) search; };

infile := dataset([
        {'one two three', [{'one'}] },
        {'Gavin Hawthorn', [{'one'}] },
        {'I went out I went in', [{'I'}] }
        ], inRec);


//test matched inside a sub query - to ensure it is evaluated in the correct place

results :=
    record
        infile.line;
        exists(dedup(infile.search, value)(value = MATCHTEXT(patWord[1])));
    end;


//Return first matching sentance that we can find
outfile1 := PARSE(infile,line,sentance,results,first,noscan);
output(outfile1);
