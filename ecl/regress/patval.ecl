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

#option ('targetClusterType', 'roxie');

x := dataset([], { string x}) : stored('x');

//set of string searchWords := [] : stored('searchWords');
set of string searchWords := set(x, x);
set of string badWords := [] : stored('badWords');


pattern ws := [' ','\t'];
pattern patStart := FIRST | ws;
pattern patEnd := LAST | ws;

token patWord := PATTERN('[a-z]+');     // deliberately lower case only
rule sentance := patStart validate(patWord, matchtext not in searchWords) validate(patWord, matchtext not in badWords) patEnd;
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


/*
infile2 := dataset('x', { dataset({string text}) child }, thor);

p := table(infile2, { dataset PARSE(infile2.child,text,sentance,results,first,noscan) });
output(p);
*/