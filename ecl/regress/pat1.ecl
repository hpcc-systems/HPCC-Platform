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

infile := dataset([
        {'one two three'},
        {'Gavin Halliday'},
        {'I went out I went in'},
        {''}
        ], { string line });


results := 
    record
        MATCHED(patWord[1]);
        MATCHTEXT(patWord[1]);
        MATCHUNICODE(patWord[1]);
        MATCHLENGTH(patWord[1]);
        MATCHPOSITION(patWord[1]);
        MATCHED(patWord[8]);
        MATCHTEXT(patWord[8]);
        MATCHLENGTH(patWord[8]);
        MATCHPOSITION(patWord[8]);
    end;


//Return first matching sentance that we can find
outfile1 := PARSE(infile,line,sentance,results,first,noscan);
output(outfile1);

//Return all matching sentances, case insignficant.
outfile2 := PARSE(infile,line,sentance,results,nocase,first,scan);
output(outfile2);

//Return first matching sentance that we can find
outfile3 := PARSE(infile,line,sentance2,results,skip(ws*),first,noscan);
output(outfile3);

//Return all matching sentances, case insignficant.
outfile4 := PARSE(infile,line,sentance2,results,nocase,scan,skip(ws*),first);
output(outfile4);
