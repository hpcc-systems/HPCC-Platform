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

pattern patWord(pattern quoteText) := quoteText PATTERN('[a-z]+') quoteText;
pattern p1 := patWord('$');
pattern p2 := patWord('!');
rule sentance0(rule head, rule tail) := head tail;
rule sentance := sentance0(p1, p2);

infile := dataset([
        {'$one$ !two! three'},
        {'gavin $Halliday$'},
        {''}
        ], { string line });


results := 
    record
        parseLib.getParseTree();
        MATCHED(p1);                        //patWord('$')[1];
        MATCHED(p2);
        MATCHTEXT(p1);
        MATCHUNICODE(p1);
        MATCHLENGTH(p1);
        MATCHPOSITION(p1);
    end;


//Return first matching sentance that we can find
outfile1 := PARSE(infile,line,sentance,results,first,scan,skip(ws*));
output(outfile1);

