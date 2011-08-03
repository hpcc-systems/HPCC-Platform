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

token patWord := PATTERN('[a-z]')+;
token noun := patWord;
pattern target := ['Gavin','Nigel','Richard','David'];
pattern pronoun := ['i','he','you','we','they'];
token verbs := ['hate','detest','loath'] opt('s' | 'es');


token subject := noun in pronoun;
token object := noun in target;
token action := patWord in verbs;

rule dislike := subject action object;

infile := dataset([
        {'I hate gavin.'},
        {'I hate gavinxxx.'},
        {'Personally I think they hate Richard'},
        {'Personally I hate Richard, he loathes Gavin'}
        ], { string line });


results :=
    record
        '\'' + MATCHTEXT(patWord[1]) + '\'';
        '\'' + MATCHTEXT(patWord[2]) + '\'';
        '\'' + MATCHTEXT(patWord[3]) + '\'';
        '\'' + MATCHTEXT(noun[1]) + '\'';
        '\'' + MATCHTEXT(noun[2]) + '\'';
        '\'' + MATCHTEXT(noun[3]) + '\'';
        '\'' + MATCHTEXT(noun/patWord[1]) + '\'';
        '\'' + MATCHTEXT(noun/patWord[2]) + '\'';
        '\'' + MATCHTEXT(noun/patWord[3]) + '\'';
        '\'' + MATCHTEXT(noun[1]/patWord[1]) + '\'';
        '\'' + MATCHTEXT(noun[1]/patWord[2]) + '\'';
        '\'' + MATCHTEXT(noun[2]/patWord[1]) + '\'';
    end;

output(PARSE(infile,line,dislike,results,scan,nocase,skip([' ',',',';','\t','.']*)));
