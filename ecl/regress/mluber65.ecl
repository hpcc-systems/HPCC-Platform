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

l :=
RECORD
    STRING30 word;
    unsigned fpos;
END;


wordds := dataset([{'MARK',1},{'LUBER',1},{'DRIMBAD',2},{'MARK',2}],l);


I := INDEX(wordds,{wordds},'~thor::wordsearchtest');


WordRec :=
RECORD
    STRING20 word;
    unsigned pos;
END;

PhraseRec :=
RECORD
    DATASET(WordRec) words;
    boolean isNot;
END;

Conjunction :=
RECORD
    DATASET(PhraseRec) phrases;
END;

words := DATASET([{'MARK'}],WordRec);
phraseds := DATASET([{wordds,false}],phraserec);
ds := DATASET([{phraseds}],Conjunction) : STORED('conj');



Doc :=
RECORD
    unsigned docID;
END;

Results :=
RECORD
    DATASET(Doc) Docs;
END;

Layout_PhrasesExpanded :=
RECORD
    Results;
    PhraseRec;
END;

Layout_wordsExpanded :=
RECORD
    Results;
    WordRec;
END;

Layout_PhrasesExpanded expandCons(PhraseRec le) :=
TRANSFORM
    SELF := le;
    SELF.docs := [];
END;

Layout_WordsExpanded expandPhrase(WordRec le) :=
TRANSFORM
    SELF := le;
    SELF.docs := [];
END;

Layout_WordsExpanded rollPhrase(Layout_WordsExpanded le, Layout_WordsExpanded ri) :=
TRANSFORM
    setofdocs := SET(le.docs,docid);

    SELF.docs := PROJECT(i(keyed(le.word=word) AND
                                                                                (fpos IN setofdocs OR setofdocs=[])),TRANSFORM(Doc,SELF.docid := LEFT.fpos));
    SELF := le;
END;

Layout_PhrasesExpanded rollCons(Layout_PhrasesExpanded le, Layout_PhrasesExpanded ri) :=
TRANSFORM
    wordsExpanded := PROJECT(le.words,expandPhrase(LEFT));

    SELF.docs := ROLLUP(wordsExpanded,true,rollPhrase(LEFT,RIGHT))[1].docs;
    SELF := le;
END;


Results proj(Conjunction le) :=
TRANSFORM
    phrasesExpanded := PROJECT(le.phrases,expandCons(LEFT));

    SELF.Docs := ROLLUP(phrasesExpanded,true,rollCons(LEFT,RIGHT))[1].docs;
END;

Docs := PROJECT(ds, proj(LEFT));

output(docs);
