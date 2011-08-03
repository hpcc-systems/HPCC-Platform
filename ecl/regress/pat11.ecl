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

token patWord(pattern quoteText) := quoteText PATTERN('[a-z]+') quoteText;
token p1 := patWord('$');
token p2 := patWord('!');
rule sentance := p1 repeat(p2);

//This needs to be allowed, but requires types to be sorted out ..
rule sentance2 := patWord('$') repeat(patWord('!'));

infile := dataset([
        {'$one$ !two! three'},
        {'gavin $Hawthorn$'},
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

results2 :=
    record
        parseLib.getParseTree();
        MATCHTEXT;
    end;


//Return first matching sentance that we can find
output(PARSE(infile,line,sentance2,results2,first,scan,skip(ws*)));
