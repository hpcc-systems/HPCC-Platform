/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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

