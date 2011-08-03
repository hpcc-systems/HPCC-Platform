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

token patWord := PATTERN('[[:alpha:]]')+;
pattern punct := pattern('[^\t]');

token noun := patWord;
token target1 := 'Gavin';
token target2 := 'Nigel';
token pronoun := ['i','he','you','we','they'];

token subject := noun;// in pronoun;
rule action1 := patWord patWord target1;
rule action2 := subject patWord target2;

rule phrase := action1 | action2;

infile := dataset([
        {'I hate gavin.'},
        {'he hates Nigel'}
        ], { string line });


results :=
    record
        MATCHTEXT(patWord[1]) + ',' + MATCHTEXT(patWord[2]) + ',' + MATCHTEXT(target1)+ ',' + MATCHTEXT(target2);
    end;

output(PARSE(infile,line,phrase,results,scan,nocase,skip(punct)));
