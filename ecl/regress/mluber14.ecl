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

ds := DATASET([{'The man sat on the chair.;'}], rs);

pattern alpha := pattern('[[:alpha:]]');
PATTERN Article := ['a', 'the', 'an'];
PATTERN EndPunct := ['.', '!', '?', ';'];
PATTERN Word := Alpha+;

TOKEN Preposition := ['of', 'on', 'above', 'below', 'under', 'over', 'in', 'during'];

RULE Noun :=  (Word penalty(5)) | (Article Word) ;
RULE Verb := Word;

RULE PP := Preposition USE(NP);
RULE NP := Noun | (SELF PP);

RULE Object := NP;
RULE IObject := PP;

RULE VP := Verb OPT(Object) OPT(IObject);

RULE Sentence := NP VP EndPunct;

NLPRec :=
RECORD
//  string tree := 'Tree: '+parseLib.getParseTree();
    MatchSentence := MATCHTEXT(Sentence);
    MatchNP := MATCHTEXT(Sentence/NP);
    MatchVP := MATCHTEXT(Sentence/VP);
    MatchVerb := MATCHTEXT(Sentence/VP/Verb);
    MatchObject := MATCHTEXT(Sentence/VP/Object);
    MatchIObject := MATCHTEXT(Sentence/VP/IObject);
END;

ParseIt := PARSE(ds, line, Sentence, NLPRec,
                    SKIP([' ','\n','\t','\r']*), PARSE, BEST, many, scan all, NOCASE);

output(ParseIt);
