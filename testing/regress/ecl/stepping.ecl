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

//Stepped global joins unsupported, see issue HPCC-8148
//skip type==thorlcr TBD

import lib_stringLib;
import $.Common.TextSearch;
import $.Setup;
import $.Setup.TS;

MaxTerms            := TS.MaxTerms;
MaxProximity        := TS.MaxProximity;
MaxWildcard     := TS.MaxWildcard;
MaxMatchPerDocument := TS.MaxMatchPerDocument;
MaxFilenameLength := TS.MaxFilenameLength;
MaxActions       := TS.MaxActions;

sourceType      := TS.sourceType;
wordCountType   := TS.wordCountType;
segmentType     := TS.segmentType;
wordPosType     := TS.wordPosType;
docPosType      := TS.docPosType;
documentId      := TS.documentId;
termType            := TS.termType;
distanceType        := TS.distanceType;
stageType       := TS.stageType;
dateType            := TS.dateType;
wordType            := TS.wordType;
wordFlags       := TS.wordFlags;
wordIdType      := TS.wordIdType;
kindType        := TS.kindType;

//Not worth executing on thor files as well..
wordIndex := Setup.Files('hthor', false).getWordIndex();

//May want the following, probably not actually implemented as an index - would save having dpos in the index, but more importantly storing it in the candidate match results because the mapping could be looked up later.
wordIndexRecord := TS.wordIndexRecord;

MaxWipIndexEntry := 4;
MaxWordsInDocument := 1000000;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

actionEnum := ENUM(
    None = 0,

//Minimal operations required to implement the searching.
    ReadWord,           // termNum, source, segment, word, wordFlagMask, wordFlagCompare,
    AndTerms,           // 
    OrTerms,            // 
    AndNotTerms,        // 
    PhraseAnd,          //
    ProximityAnd,       // distanceBefore, distanceAfter
    MofNTerms,          // minMatches, maxMatches

    PassThrough,
    Last,

    //The following are only used in the production
    FlagModifier,       // wordFlagMask, wordFlagCompare
    QuoteModifier,      // 
    Max
);

//  FAIL(stageType, 'Missing entry: ' + (string)action));

boolean definesTerm(actionEnum action) := 
    (action in [actionEnum.ReadWord]);

stageRecord := { stageType stage };
stageSet := set of stageType;

searchRecord := 
            RECORD
stageType       stage;
actionEnum      action;
//termType      term;
dataset(stageRecord) inputs{maxcount(maxTerms)};

distanceType    maxWip;
distanceType    maxWipChild;
distanceType    maxWipLeft;
distanceType    maxWipRight;

//The item being searched for
wordType        word;
wordFlags       wordFlagMask;
wordFlags       wordFlagCompare;
sourceType      source;
segmentType     segment;

//Modifiers for the connector/filter
distanceType    maxDistanceRightBeforeLeft;
distanceType    maxDistanceRightAfterLeft;
unsigned1       minMatches;
unsigned1       maxMatches;

            END;


StageSetToDataset(stageSet x) := dataset(x, stageRecord);
StageDatasetToSet(dataset(stageRecord) x) := set(x, stage);

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Matches

childMatchRecord := RECORD
wordPosType         wpos;
wordPosType         wip;
                END;

matchRecord :=  RECORD
documentId          doc;
segmentType         segment;
wordPosType         wpos;
wordPosType         wip;
dataset(childMatchRecord) children{maxcount(MaxProximity)};
                END;

createChildMatch(wordPosType wpos, wordPosType wip) := transform(childMatchRecord, self.wpos := wpos; self.wip := wip);
SetOfInputs := set of dataset(matchRecord);

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Functions which are helpful for hand constructing queries...

CmdReadWord(wordType word, sourceType source = 0, segmentType segment = 0, wordFlags wordFlagMask = 0, wordFlags wordFlagCompare = 0) :=
    TRANSFORM(searchRecord,
                SELF.action := actionEnum.ReadWord;
                SELF.source := source;
                SELF.segment := segment;
                SELF.word := word;
                SELF.wordFlagMask := wordFlagMask;              
                SELF.wordFlagCompare:= wordFlagCompare;
                SELF.maxWip := 1;
                SELF := []);

defineCmdTermCombineTerm(actionEnum action, stageSet inputs, distanceType maxDistanceRightBeforeLeft = 0, distanceType maxDistanceRightAfterLeft = 0) :=
    TRANSFORM(searchRecord,
                SELF.action := action;
                SELF.inputs := StageSetToDataset(inputs);
                SELF.maxDistanceRightBeforeLeft := maxDistanceRightBeforeLeft;
                SELF.maxDistanceRightAfterLeft := maxDistanceRightAfterLeft;
                SELF.maxWip := 1;
                SELF.maxWipLeft := 1;
                SELF.maxWipRight := 1;
                SELF := []);

CmdTermAndTerm(stageType leftStage, stageType rightStage) :=
    defineCmdTermCombineTerm(actionEnum.AndTerms, [leftStage, rightStage]);

CmdAndTerms(stageSet stages) :=
    defineCmdTermCombineTerm(actionEnum.AndTerms, stages);

CmdTermAndNotTerm(stageType leftStage, stageType rightStage) :=
    defineCmdTermCombineTerm(actionEnum.AndNotTerms, [leftStage, rightStage]);

CmdTermAndNotTerms(stageSet stages) :=
    defineCmdTermCombineTerm(actionEnum.AndNotTerms, stages);

CmdMofNTerms(stageSet stages, unsigned minMatches, unsigned maxMatches = 999999999) :=
    TRANSFORM(searchRecord,
                SELF.action := actionEnum.MofNTerms;
                SELF.inputs := StageSetToDataset(stages);
                SELF.minMatches := minMatches;
                SELF.maxMatches := maxMatches;
                SELF.maxWip := 1;
                SELF := []);

CmdPhraseAnd(stageSet stages) :=
    defineCmdTermCombineTerm(actionEnum.PhraseAnd, stages);

CmdProximityAnd(stageType leftStage, stageType rightStage, distanceType maxDistanceRightBeforeLeft, distanceType maxDistanceRightAfterLeft) :=
    defineCmdTermCombineTerm(actionEnum.ProximityAnd, [leftStage, rightStage], maxDistanceRightBeforeLeft, maxDistanceRightAfterLeft);

CmdTermOrTerm(stageType leftStage, stageType rightStage) :=
    defineCmdTermCombineTerm(actionEnum.OrTerms, [leftStage, rightStage]);

CmdOrTerms(stageSet stages) :=
    defineCmdTermCombineTerm(actionEnum.OrTerms, stages);


//-------------------------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------------------------
//---------------------------------------- Code for executing queries -----------------------------------------
//-------------------------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------------------------

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Matching helper functions

matchSingleWordFlags(wordIndex wIndex, searchRecord search) :=
    keyed(search.segment = 0 or wIndex.segment = search.segment, opt) AND
    ((wIndex.flags & search.wordFlagMask) = search.wordFlagCompare);

matchSingleWord(wordIndex wIndex, searchRecord search) :=
    keyed(wIndex.kind = kindType.TextEntry and wIndex.word = search.word) AND
    matchSingleWordFlags(wIndex, search);

matchFirstWord(wordIndex wIndex, searchRecord search) :=
    keyed(search.source = 0 OR TS.docMatchesSource(wIndex.doc, search.source), opt);

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadWord

doReadWord(searchRecord search) := FUNCTION

    matches := sorted(wordIndex, doc, segment, wpos, wip)(
                        matchSingleWord(wordIndex, search) AND
                        matchFirstWord(wordIndex, search));

    matchRecord createMatchRecord(wordIndexRecord ds) := transform
        self := ds;
        self.children := []
    end;

    steppedMatches := stepped(matches, doc, segment, wpos);

    projected := project(steppedMatches, createMatchRecord(left));

    //Because word is single valued, then must be sorted by document
    sortedMatches := sorted(projected, doc, segment, wpos, wip, assert);

    return projected;
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// AndTerms

doAndTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return mergejoin(inputs, STEPPED(left.doc = right.doc), doc, segment, wpos, dedup);     // MORE  option to specify priority?
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// OrTerms

doOrTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return merge(inputs, doc, segment, wpos, dedup);        // MORE  option to specify priority?
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// AndNotTerms

doAndNotTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return mergejoin(inputs, STEPPED(left.doc = right.doc), doc, segment, wpos, left only);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// PhraseAnd

steppedPhraseCondition(matchRecord l, matchRecord r, distanceType maxWip) := 
        (l.doc = r.doc) and (l.segment = r.segment) and
        (r.wpos between l.wpos+1 and l.wpos+maxWip);

doPhraseAnd(searchRecord search, SetOfInputs inputs) := FUNCTION

    steppedCondition(matchRecord l, matchRecord r) := steppedPhraseCondition(l, r, search.maxWipLeft);

    condition(matchRecord l, matchRecord r) :=  
        (r.wpos = l.wpos + l.wip);

    matchRecord createMatch(matchRecord l, dataset(matchRecord) allRows) := transform
        self.wip := sum(allRows, wip);
        self := l;
    end;

    matches := join(inputs, STEPPED(steppedCondition(left, right)) and condition(LEFT, RIGHT), createMatch(LEFT, ROWS(LEFT)), sorted(doc, segment, wpos));

    ret := sorted(matches, doc, segment, wpos, wip, assert);

    return ret;
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// ProximityAnd

steppedProximityCondition(matchRecord l, matchRecord r, distanceType maxWipLeft, distanceType maxWipRight, distanceType maxDistanceRightBeforeLeft, distanceType maxDistanceRightAfterLeft) := function
        // if maxDistanceRightBeforeLeft is < 0 it means it must follow, so don't add maxWipRight
        maxRightBeforeLeft := IF(maxDistanceRightBeforeLeft >= 0, maxDistanceRightBeforeLeft + maxWipRight, maxDistanceRightBeforeLeft);
        maxRightAfterLeft := IF(maxDistanceRightAfterLeft >= 0, maxDistanceRightAfterLeft + maxWipLeft, maxDistanceRightAfterLeft);

        return 
            (l.doc = r.doc) and (l.segment = r.segment) and
            (r.wpos + maxRightBeforeLeft >= l.wpos) and             // (right.wpos + right.wip + maxRightBeforeLeft >= left.wpos)
            (r.wpos <= l.wpos + (maxRightAfterLeft));               // (right.wpos <= left.wpos + left.wip + maxRightAfterLeft)
end;

doProximityAnd(searchRecord search, SetOfInputs inputs) := FUNCTION

    steppedCondition(matchRecord l, matchRecord r) := steppedProximityCondition(l, r, search.maxWipLeft, search.maxWipRight, search.maxDistanceRightBeforeLeft, search.maxDistanceRightAfterLeft);

    condition(matchRecord l, matchRecord r) :=  
        (r.wpos + r.wip + search.maxDistanceRightBeforeLeft >= l.wpos) and
        (r.wpos <= l.wpos + l.wip + search.maxDistanceRightAfterLeft);

    overlaps(wordPosType wpos, childMatchRecord r) := (wpos between r.wpos and r.wpos + (r.wip - 1));

    createMatch(matchRecord l, matchRecord r) := function
    
        wpos := if(l.wpos < r.wpos, l.wpos, r.wpos);
        wend := if(l.wpos + l.wip > r.wpos + r.wip, l.wpos + l.wip, r.wpos + r.wip);

        rawLeftChildren := IF(exists(l.children), l.children, dataset(row(createChildMatch(l.wpos, l.wip))));
        rawRightChildren := IF(exists(r.children), r.children, dataset(row(createChildMatch(r.wpos, r.wip))));
        leftChildren := sorted(rawLeftChildren, wpos, wip, assert);
        rightChildren := sorted(rawRightChildren, wpos, wip, assert);
        anyOverlaps := exists(join(leftChildren, rightChildren,
                               overlaps(left.wpos, right) or overlaps(left.wpos+(left.wip-1), right) or
                               overlaps(right.wpos, left) or overlaps(right.wpos+(right.wip-1), left), all));

    //Check for any overlaps between the words, should be disjoint.
        matchRecord matchTransform := transform, skip(anyOverlaps)
            self.wpos := wpos;
            self.wip := wend - wpos;
            self.children := merge(leftChildren, rightChildren, dedup);
            self := l;
        end;

        return matchTransform;
    end;

    matches := join(inputs, STEPPED(steppedCondition(left, right)) and condition(LEFT, RIGHT), createMatch(LEFT, RIGHT), sorted(doc, segment, wpos));

    ret := sorted(matches, doc, segment, wpos, wip, assert);

    return ret;
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// AndTerms

doMofNTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return mergejoin(inputs, STEPPED(left.doc = right.doc), doc, segment, wpos, wip, dedup, mofn(search.minMatches, search.maxMatches));        // MORE  option to specify priority?
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////

processStage(searchRecord search, SetOfInputs allInputs) := function
    inputs:= RANGE(allInputs, StageDatasetToSet(search.inputs));
    result := case(search.action,
        actionEnum.ReadWord             => doReadWord(search),
        actionEnum.AndTerms             => doAndTerms(search, inputs),
        actionEnum.OrTerms              => doOrTerms(search, inputs),
        actionEnum.AndNotTerms          => doAndNotTerms(search, inputs),
        actionEnum.PhraseAnd            => doPhraseAnd(search, inputs),
        actionEnum.ProximityAnd         => doProximityAnd(search, inputs),
        actionEnum.MofNTerms            => doMofNTerms(search, inputs),
        dataset([], matchRecord));
    return result;
end;

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Code to actually execute the query:

convertToUserOutput(dataset(matchRecord) results) := function

    simpleUserOutputRecord :=
            record
    unsigned2           source;
    unsigned6           subDoc;
    wordPosType         wpos;
    wordPosType         wip;
            end;

    simpleUserOutputRecord createUserOutput(matchRecord l) := transform
            self.source := TS.docid2source(l.doc);
            self.subDoc := TS.docid2doc(l.doc);
            SELF := l;
        END;

    return project(results, createUserOutput(left));
end;

ExecuteQuery(dataset(searchRecord) queryDefinition, dataset(matchRecord) initialResults = dataset([], matchRecord)) := function

    executionPlan := global(queryDefinition, opt, few);         // Store globally for efficient access

    results := graph(initialResults, count(executionPlan), processStage(executionPlan[NOBOUNDCHECK COUNTER], rowset(left)));

    userOutput := convertToUserOutput(results);

    return userOutput;

end;


executeReadWord(wordType word, sourceType source = 0, segmentType segment = 0, wordFlags wordFlagMask = 0, wordFlags wordFlagCompare = 0) :=
    doReadWord(row(CmdReadWord(word, source, segment, wordFlagMask, wordFlagCompare)));

executeAndTerms(SetOfInputs stages) :=
    doAndTerms(row(CmdAndTerms([])), stages);

executeAndNotTerms(SetOfInputs stages) :=
    doAndNotTerms(row(CmdTermAndNotTerms([])), stages);

executeMofNTerms(SetOfInputs stages, unsigned minMatches, unsigned maxMatches = 999999999) :=
    doMofNTerms(row(CmdMofNTerms([], minMatches, maxMatches)), stages);
    
executeOrTerms(SetOfInputs stages) :=
    doOrTerms(row(CmdOrTerms([])), stages);

executePhrase(SetOfInputs stages) :=
    doPhraseAnd(row(CmdPhraseAnd([])), stages);

executeProximity(SetOfInputs stages, distanceType maxDistanceRightBeforeLeft, distanceType maxDistanceRightAfterLeft) :=
    doProximityAnd(row(CmdProximityAnd(0,0, maxDistanceRightBeforeLeft, maxDistanceRightAfterLeft)), stages);

///////////////////////////////////////////////////////////////////////////////////////////////////////////

#if (0)
//Use a dynamic graph....
q1 := dataset([
            CmdReadWord('black'),
            CmdReadWord('sheep'),
            CmdAndTerms([1,2])
            ]);

output(ExecuteQuery(q1));

#else

//Same attributes, using an explicit graph

x1 := executeReadWord('black');
x2 := executeReadWord('sheep');
x3 := executeReadWord('white');
x4 := executeAndTerms([x1, x2]);
x5 := executeAndNotTerms([x1, x2]);
x6 := executeMofNTerms([x1,x2,x3],2);
x7 := executeMofNTerms([x1,x2,x3],2,2);
sequential (
 output(convertToUserOutput(x4)),
 output(convertToUserOutput(x5)),
 output(convertToUserOutput(x6)),
 output(convertToUserOutput(x7))
)

#end
