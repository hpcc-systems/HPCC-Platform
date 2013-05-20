/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

//UseStandardFiles
//UseIndexes
//varskip payload
//varskip varload
//varskip trans
//varskip dynamic

//Multi level smart stepping, with priorities in the correct order

extractWord(string searchWord, unsigned searchPriority) := STEPPED(TS_searchIndex(kind=1 AND word=searchWord), doc, PRIORITY(searchPriority),HINT(maxseeklookahead(50)));


searchRec := RECORD
    STRING word;
    UNSIGNED prio;
END;

outputRecord := RECORDOF(extractWord);

doSearch(DATASET(searchRec) search) := FUNCTION

    numWords := COUNT(search);

    doAction(SET OF DATASET(outputRecord) prev, UNSIGNED step) := FUNCTION
        nextWord := extractWord(search[NOBOUNDCHECK step].word, search[NOBOUNDCHECK step].prio);
        sillyIntegerDs := DATASET(numWords, TRANSFORM({UNSIGNED x}, SELF.x := COUNTER));
        sillyIntegerSet := SET(sillyIntegerDs, x);
        doJoin := MERGEJOIN(RANGE(prev, sillyIntegerSet), STEPPED(LEFT.doc = RIGHT.doc), SORTED(doc));
        RETURN IF (step <= numWords, nextWord, doJoin);
    END;

    nullInput := DATASET([], outputRecord);
    RETURN GRAPH(nullInput, numWords+1, doAction(ROWSET(LEFT), COUNTER), PARALLEL);
END;

search := DATASET([{'jericho',1},{'walls',2},{'the',3}], searchRec);

x := doSearch(search);
output(SUBSORT(x, { doc }, { word, wpos }));
