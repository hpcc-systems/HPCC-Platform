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
//Ensure that local is correctly added to sub queries and all children.

mainRecord :=
        RECORD
string20            word;
unsigned8           doc;
unsigned8           wpos;
unsigned4           relevance;
        END;

wordDataset := dataset('~words.d00',mainRecord,THOR);

wordKey := INDEX(wordDataset , { wordDataset }, {}, 'word.idx');
wordKey2 := INDEX(wordDataset , { wordDataset }, {}, 'word2.idx');


wordKey t(wordKey l) := transform
    self.doc := wordKey2(word = l.word)[1].doc;
    self := l;
    end;
x := project(wordKey, t(left));

output(local(x));


//Ensure join(Local(key)) is treated as a local join
j := join(local(wordKey), local(wordKey2), left.word = right.word and left.doc = right.doc, transform(mainRecord, self.wpos := right.wpos; self := left));
output(j);


//Ensure join(noLocal(key)) is treated as a nonlocal join
j2 := join(wordKey, nolocal(wordKey2), left.word = right.word and left.doc = right.doc, transform(mainRecord, self.wpos := right.wpos; self := left));
output(local(j2));
