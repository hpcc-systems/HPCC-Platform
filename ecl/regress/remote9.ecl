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
//Check thisnode is handled, this time inside a nested allnodes, with thisnode repeated

mainRecord := 
        RECORD
string20            word;
unsigned8           doc;
unsigned8           wpos;
unsigned4           relevance;
        END;


wordDataset := dataset('~words.d00',mainRecord,THOR);
matchedAlready := dataset('~matched.d00',mainRecord,THOR);
bestPrevious1 := topn(matchedAlready, 1, -relevance);

wordKey := INDEX(wordDataset , { wordDataset }, {}, 'word.idx', distributed(hash(doc)));
wordKey2 := INDEX(wordDataset , { wordDataset }, {}, 'word2.idx');


bestPrevious := project(bestPrevious1, transform(recordof(wordKey2), self := left; self := []));

wordKey t(wordKey l) := transform
    matches := wordKey2(word = l.word);
    best1 := topn(thisnode(thisnode(bestPrevious)) + matches, 1, -relevance);
    best2 := topn(allnodes(local(best1)), 1, -relevance);
    self.doc := best2[1].doc;
    self := l;
    end;

q := project(wordKey, t(left));
output(allnodes(q));


