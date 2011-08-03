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
//Check thisnode is handled as a scalar
//(using a mock up of the main text processing loop...)

mainRecord :=
        RECORD
string20            word;
unsigned8           doc;
unsigned8           wpos;
unsigned4           relevance;
        END;


childRecord :=
        RECORD
string20            word;
unsigned8           doc;
unsigned8           wpos;
        END;


resultRecord    := record
unsigned                seq;
string20                search;
dataset(childRecord)    children;
                   end;

wordDataset := dataset('~words.d00',mainRecord,THOR);

wordKey := INDEX(wordDataset , { wordDataset }, {}, 'word.idx', distributed(hash(doc)));



handle(dataset(childRecord) prev, string _searchWord) := function

    searchWord := thisnode(_searchWord);
    matches := wordKey(word=searchWord);
    return prev + allnodes(project(matches, transform(childRecord, self := left)));
end;


initial := dataset('i', resultRecord, thor);
p1 := project(initial, transform(resultRecord, self.children := handle(left.children, left.search); self := left));
output(p1);

