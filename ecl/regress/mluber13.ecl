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

rs :=
RECORD
    STRING line;
END;

pattern Alpha := pattern('[[:alpha:]]');

ds := DATASET([{'area. Most of eastern and central Pennsylvania is drained by the Susquehanna and Delaware systems. The western part of the state is drained by the Allegheny and Monongahela rivers, which join at Pittsburgh'}], rs);

PATTERN Back  := (ANY NOT IN Alpha) | LAST;
PATTERN Front := (ANY NOT IN Alpha) | FIRST;

PATTERN Word := Alpha+;
PATTERN TrueWord := ((Word BEFORE Back) AFTER Front);

Matches :=
    record
        STRING50 wrd := MATCHTEXT(Word);
        UNSIGNED4 MatchPos  := MATCHPOSITION(Word);
        UNSIGNED2 MatchLen  := MATCHLENGTH(Word);
    end;

found := PARSE(ds, line, TrueWord, Matches, BEST, MANY, MAX);



output(found,, 'a.out',csv,overwrite);

token Word2 := Alpha+;

Matches2 :=
    record
        STRING50 wrd := MATCHTEXT(Word2);
        UNSIGNED4 MatchPos  := MATCHPOSITION(Word2);
        UNSIGNED2 MatchLen  := MATCHLENGTH(Word2);
    end;

found2 := PARSE(ds, line, Word2, Matches2);
output(found2,, 'b.out',csv,overwrite);


