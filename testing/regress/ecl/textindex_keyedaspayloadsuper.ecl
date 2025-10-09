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

//version multiPart=false
//version multiPart=true
//version multiPart=false,variant='mixed'
//version multiPart=true,variant='mixed'

//NOTE: This test does not currently work on thor, see HPCC-34803 to address that issue
//nothor

// Test reading an index with the ecl indicating that a field is in the payload, but it is actually keyed.
// from a superfile - default the indexes are the same version, mixed - one has the key in the payload,
// and one in the key

#onwarning(4523, ignore);

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
variant := #IFDEFINED(root.variant, '');
numJoins := #IFDEFINED(root.numJoins, 1);

//--- end of version configuration ---

import $.setup;
import setup.ts;
import Std.File AS FileServices;

files := setup.files(multiPart, false);

searchIndex := files.getSearchIndexVariant('');
dupSearchIndex := files.getSearchIndexVariant('default');
nowipIndex := files.getSearchIndexVariant('nowip');

rec := recordof(searchIndex);

//Ensure the prefix is constant, otherwise the child query keeps re-resolving the file
prefix := '~regress::' + __TARGET_PLATFORM__ + '::' + MAP(multiPart => 'multi', 'single') + '_' + variant + '::';
supername := prefix + 'ti_superfile1'+variant;

//The actual format of the index is the following - wip is keyed:
//  searchIndex := index({ kindType kind, wordType word, documentId doc, segmentType segment, wordPosType wpos, indexWipType wip } , { wordFlags flags, wordType original, docPosType dpos}, '~DoesNotExist');

reorderedIndex  := index({ rec.kind, rec.word, rec.doc, rec.segment, rec.wpos }, { rec }, supername);

createSample(unsigned i, unsigned num, unsigned numRows) := FUNCTION

    //Add a keyed filter to ensure that no splitter is generated.
    //The splitter performs pathologically on roxie - it may be worth further investigation
    filtered := files.getSearchSource()(HASH32(kind, word, doc, segment, wpos) % num = i, keyed(word != ''));
    inputFile := choosen(filtered, numRows);
    j := JOIN(inputFile, reorderedIndex,
            (LEFT.kind = RIGHT.kind) AND
            (LEFT.word = RIGHT.word) AND
            (LEFT.doc = RIGHT.doc) AND
            (LEFT.segment = RIGHT.segment) AND
            (LEFT.wpos = RIGHT.wpos), ATMOST(10));
    RETURN NOFOLD(j);
END;

//Use index count operations to access the index
createSample2(unsigned i, unsigned num, unsigned numRows) := FUNCTION

    //Add a keyed filter to ensure that no splitter is generated.
    //The splitter performs pathologically on roxie - it may be worth further investigation
    filtered := files.getSearchSource()(HASH32(kind, word, doc, segment, wpos) % num = i, keyed(word != ''));
    inputFile := choosen(filtered, numRows);
    outRec := { recordof(filtered); unsigned matches; };
    outRec t(inputFile l) := TRANSFORM
        SELF.matches := COUNT(reorderedIndex(
            (kind = l.kind) AND
            (word = l.word) AND
            (doc = l.doc) AND
            (segment = l.segment) AND
            (wpos = l.wpos)));
        SELF := l;
    END;
    p := PROJECT(inputFile, t(LEFT));
    RETURN NOFOLD(p);
END;

//Use index read operations to access the index
createSample3(unsigned i, unsigned num, unsigned numRows) := FUNCTION

    //Add a keyed filter to ensure that no splitter is generated.
    //The splitter performs pathologically on roxie - it may be worth further investigation
    filtered := files.getSearchSource()(HASH32(kind, word, doc, segment, wpos) % num = i, keyed(word != ''));
    inputFile := choosen(filtered, numRows);
    outRec := { recordof(filtered); unsigned matches; };
    outRec t(inputFile l) := TRANSFORM
        SELF.matches := COUNT(NOFOLD(reorderedIndex(
            (kind = l.kind) AND
            (word = l.word) AND
            (doc = l.doc) AND
            (segment = l.segment) AND
            (wpos = l.wpos))));
        SELF := l;
    END;
    p := PROJECT(inputFile, t(LEFT));
    RETURN NOFOLD(p);
END;


createSamples(iters, numRows) := FUNCTIONMACRO
    o := PARALLEL(
    #DECLARE (count)
    #SET (count, 0)
    #LOOP
        #IF (%count%>=iters)
            #BREAK
        #END
        output(count(createSample(%count%, iters, numRows)) - 2 * numRows),
        output(SUM(createSample2(%count%, iters, numRows), matches) - 2 * numRows),
        output(SUM(createSample3(%count%, iters, numRows), matches) - 2 * numRows),
        #SET (count, %count%+1)
    #END
        output('Done')
    );
    RETURN o;
ENDMACRO;

SEQUENTIAL(
    FileServices.DeleteSuperFile(supername),
    FileServices.CreateSuperFile(supername),
    FileServices.StartSuperFileTransaction(),
    IF (variant='mixed',
        FileServices.AddSuperFile(supername, __nameof__(nowipIndex)),
        FileServices.AddSuperFile(supername, __nameof__(dupSearchIndex))),
    FileServices.FinishSuperFileTransaction(),
    FileServices.AddSuperFile(supername, __nameof__(searchIndex)),
    createSamples(numJoins, 6000),
    FileServices.DeleteSuperFile(supername),
);
