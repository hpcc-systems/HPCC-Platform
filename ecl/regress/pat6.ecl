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

import patLib;

pattern ws := [' ','\t'];
pattern patStart := FIRST | ws;
pattern patEnd := LAST | ws;

rule sentance := patStart patLib.patWord repeat(ws patLib.patWord) patEnd;
rule sentance2 := repeat(patLib.patWord);

infile := dataset([
        {'one two three'},
        {'Gavin Hawthorn'},
        {'I went out I went in'},
        {''}
        ], { string line });


results :=
    record
        MATCHED(patLib.patWord[1]);
        MATCHTEXT(patLib.patWord[1]);
        MATCHLENGTH(patLib.patWord[1]);
        MATCHPOSITION(patLib.patWord[1]);
    end;


//Return first matching sentance that we can find
outfile1 := PARSE(infile,line,sentance,results);
output(outfile1);

//Return all matching sentances, case insignficant.
outfile2 := PARSE(infile,line,sentance,results,nocase,scan);
output(outfile2);

//Return first matching sentance that we can find
outfile3 := PARSE(infile,line,sentance2,results,skip(ws));
output(outfile3);

//Return all matching sentances, case insignficant.
outfile4 := PARSE(infile,line,sentance2,results,nocase,scan,skip(ws));
output(outfile4);
