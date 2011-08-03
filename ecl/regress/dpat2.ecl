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

 d := dataset([{'In the beginning God created the heaven and the earth.'},{'I like cheese'}],
{string1000 line});

token Word := PATTERN('[-a-zA-Z\']+');
pattern Verbs := ['created'];
rule Verb := Word IN Verbs;
pattern Articles := ['a','the'];
rule article := Word IN Articles;
pattern Conjunctions := ['and'];
rule conjunction := Word IN Conjunctions;
rule NounPhraseComponent := Word penalty(1) | article Word;
rule NounPhrase := NounPhraseComponent opt(conjunction NounPhraseComponent);
rule clause := NounPhrase Verb NounPhrase;

results :=
    record
        MATCHED;
        Subject :=  MATCHTEXT(NounPhrase[1]);
        Object  :=  MATCHTEXT(NounPhrase[2]);
        Action :=  MATCHTEXT(Verb[1]);
    end;

outfile1 := PARSE(d,line,clause,results,best,max,scan,atmost(1),skip([' ','\t',',','.',';',':']));

count(outfile1);
output(choosen(outfile1,1000),named('Standard'));

output(PARSE(d,line,clause,results,best,max,scan,not matched,skip([' ','\t',',','.',';',':'])),named('NotMatched'));
output(PARSE(d,line,clause,results,best,max,scan,not matched only,skip([' ','\t',',','.',';',':'])),named('NotMatchedOnly'));
