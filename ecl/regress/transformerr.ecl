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

#option ('targetClusterType','roxie');

import ghoogle;
#option ('checkAsserts',false);
import lib_stringLib;

MaxTerms            := ghoogle.MaxTerms;
MaxProximity        := ghoogle.MaxProximity;
MaxWildcard     := ghoogle.MaxWildcard;
MaxMatchPerDocument := ghoogle.MaxMatchPerDocument;
MaxFilenameLength := ghoogle.MaxFilenameLength;
MaxActions       := ghoogle.MaxActions;

sourceType      := ghoogle.sourceType;
wordCountType   := ghoogle.wordCountType;
segmentType     := ghoogle.segmentType;
wordPosType     := ghoogle.wordPosType;
docPosType      := ghoogle.docPosType;
documentId      := ghoogle.documentId;
termType            := ghoogle.termType;
distanceType        := ghoogle.distanceType;
stageType       := ghoogle.stageType;
dateType            := ghoogle.dateType;
charPosType     := ghoogle.charPosType;
wordType            := ghoogle.wordType;
wordFlags       := ghoogle.wordFlags;
wordIdType      := ghoogle.wordIdType;
corpusFlags     := ghoogle.corpusFlags;

NameCorpusIndex     := ghoogle.NameCorpusIndex;
NameWordIndex       := ghoogle.NameWordIndex;
NameSentenceIndex   := ghoogle.NameSentenceIndex;
NameParagraphIndex  := ghoogle.NameParagraphIndex;
NameDocMetaIndex        := ghoogle.NameDocMetaIndex;
NameDateDocIndex        := ghoogle.NameDateDocIndex;
NameDocPosIndex     := ghoogle.NameDocPosIndex;
NameTokenisedDocIndex:= ghoogle.NameTokenisedDocIndex;
NameTokenIndex      := ghoogle.NameTokenIndex;

corpusIndex     := ghoogle.corpusIndex;
wordIndex       := ghoogle.wordIndex;
sentenceIndex   := ghoogle.sentenceIndex;
paragraphIndex  := ghoogle.paragraphIndex;
docMetaIndex        := ghoogle.docMetaIndex;
dateDocIndex        := ghoogle.dateDocIndex;

//May want the following, probably not actually implemented as an index - would save having dpos in the index, but more importantly storing it in the candidate match results because the mapping could be looked up later.
docPosIndex     := ghoogle.docPosIndex;
wordIndexRecord := ghoogle.wordIndexRecord;

MaxWipIndexEntry := 4;
MaxWordsInDocument := 1000000;
MaxWordsInSet      := 20;

///////////////////////////////////////////////////////////////////////////////////////////////////////////
//cloned from ghoogle.
sourceType docid2source(documentId x) := (x >> 48);
unsigned8 docid2doc(documentId x) := (x & 0xFFFFFFFFFFFF);
documentId createDocId(sourceType source, unsigned6 doc) := (documentId)(((unsigned8)source << 48) | doc);
boolean docMatchesSource(unsigned8 docid, sourceType source) := (docid between createDocId(source,0) and (documentId)(createDocId(source+1,0)-1));

///////////////////////////////////////////////////////////////////////////////////////////////////////////

actionEnum := ENUM(
    None = 0,

//Minimal operations required to implement the searching.
    ReadWord,           // termNum, source, segment, word, wordFlagMask, wordFlagCompare,
    ReadWordSet,        // termNum, source, segment, words, wordFlagMask, wordFlagCompare,
    AndTerms,           //
    OrTerms,            //
    AndNotTerms,        //
    PhraseAnd,          //
    ProximityAnd,       // distanceBefore, distanceAfter
    MofNTerms,          // minMatches, maxMatches
    RankMergeTerms,     // left outer join
    RollupByDocument,   // grouped rollup by document.
    NormalizeMatch,     // Normalize proximity records.

//The following aren't very sensible as far as text searching goes, but are here to test the underlying functionality
    AndJoinTerms,       // join on non-proximity
    AndNotJoinTerms,    //
    MofNJoinTerms,      // minMatches, maxMatches
    RankJoinTerms,      // left outer join
    ProximityMergeAnd,  // merge join on proximity

    PassThrough,
    Last,

    //The following are only used in the production
    FlagModifier,       // wordFlagMask, wordFlagCompare
    QuoteModifier,      //
    Max
);

//  FAIL(stageType, 'Missing entry: ' + (string)action));

boolean definesTerm(actionEnum action) :=
    (action in [actionEnum.ReadWord, actionEnum.ReadWordSet]);

stageRecord := { stageType stage };
wordRecord := { wordType word; };
wordSet := set of wordType;
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
dataset(wordRecord) words{maxcount(maxWordsInSet)};
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

simpleUserOutputRecord :=
        record
string  name{maxlength(MaxFilenameLength)};
wordPosType         wpos;
wordPosType         wip;
        end;


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
    keyed(wIndex.word = search.word) AND
    matchSingleWordFlags(wIndex, search);

matchManyWord(wordIndex wIndex, searchRecord search) :=
    keyed(wIndex.word in set(search.words, word)) AND
    matchSingleWordFlags(wIndex, search);

matchFirstWord(wordIndex wIndex, searchRecord search) :=
    keyed(search.source = 0 OR docMatchesSource(wIndex.doc, search.source), opt);

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

    return projected;
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadWord

doReadWordSet(searchRecord search) := FUNCTION

    matches := sorted(wordIndex, doc, segment, wpos, wip)(
                        matchManyWord(wordIndex, search) AND
                        matchFirstWord(wordIndex, search));

    matchRecord createMatchRecord(wordIndexRecord ds) := transform
        self := ds;
        self.children := []
    end;

    steppedMatches := stepped(matches, doc, segment, wpos);

    projected := project(steppedMatches, createMatchRecord(left));

    return projected;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// OrTerms

doOrTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return merge(inputs, doc, segment, wpos, dedup);        // MORE  option to specify priority?
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// AndTerms

doAndTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return mergejoin(inputs, STEPPED(left.doc = right.doc), doc, segment, wpos, dedup);     // MORE  option to specify priority?
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// AndNotTerms

doAndNotTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return mergejoin(inputs, STEPPED(left.doc = right.doc), doc, segment, wpos, left only);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// RankMergeTerms

doRankMergeTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return mergejoin(inputs, STEPPED(left.doc = right.doc), doc, segment, wpos, left outer);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// M of N

doMofNTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return mergejoin(inputs, STEPPED(left.doc = right.doc), doc, segment, wpos, wip, dedup, mofn(search.minMatches, search.maxMatches));        // MORE  option to specify priority?
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Join varieties - primarily for testing

//Note this testing transform wouldn't work correctly with proximity operators as inputs.
createDenormalizedMatch(matchRecord l, dataset(matchRecord) matches) := transform

    wpos := min(matches, wpos);
    wend := max(matches, wpos + wip);

    self.wpos := wpos;
    self.wip := wend - wpos;
    self.children := normalize(matches, 1, createChildMatch(LEFT.wpos, LEFT.wip));
    self := l;
end;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// AndJoinTerms

doAndJoinTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return join(inputs, STEPPED(left.doc = right.doc), createDenormalizedMatch(LEFT, ROWSET(left)), sorted(doc));
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// AndNotJoinTerms

doAndNotJoinTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return join(inputs, STEPPED(left.doc = right.doc), createDenormalizedMatch(LEFT, ROWSET(left)), sorted(doc, segment, wpos), left only);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// RankJoinTerms

doRankJoinTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return join(inputs, STEPPED(left.doc = right.doc), createDenormalizedMatch(LEFT, ROWSET(left)), sorted(doc, segment, wpos), left outer);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// M of N

doMofNJoinTerms(searchRecord search, SetOfInputs inputs) := FUNCTION
    return join(inputs, STEPPED(left.doc = right.doc), createDenormalizedMatch(LEFT, ROWSET(left)), sorted(doc, segment, wpos), mofn(search.minMatches, search.maxMatches));
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

    return matches;
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

    return matches;
END;


doProximityMergeAnd(searchRecord search, SetOfInputs inputs) := FUNCTION

    steppedCondition(matchRecord l, matchRecord r) := steppedProximityCondition(l, r, search.maxWipLeft, search.maxWipRight, search.maxDistanceRightBeforeLeft, search.maxDistanceRightAfterLeft);

    condition(matchRecord l, matchRecord r) :=
        (r.wpos + r.wip + search.maxDistanceRightBeforeLeft >= l.wpos) and
        (r.wpos <= l.wpos + l.wip + search.maxDistanceRightAfterLeft);

    overlaps(wordPosType wpos, childMatchRecord r) := (wpos between r.wpos and r.wpos + (r.wip - 1));

    anyOverlaps (matchRecord l, matchRecord r) := function

        wpos := if(l.wpos < r.wpos, l.wpos, r.wpos);
        wend := if(l.wpos + l.wip > r.wpos + r.wip, l.wpos + l.wip, r.wpos + r.wip);

        rawLeftChildren := IF(exists(l.children), l.children, dataset(row(createChildMatch(l.wpos, l.wip))));
        rawRightChildren := IF(exists(r.children), r.children, dataset(row(createChildMatch(r.wpos, r.wip))));
        leftChildren := sorted(rawLeftChildren, wpos, wip, assert);
        rightChildren := sorted(rawRightChildren, wpos, wip, assert);
        return exists(join(leftChildren, rightChildren,
                               overlaps(left.wpos, right) or overlaps(left.wpos+(left.wip-1), right) or
                               overlaps(right.wpos, left) or overlaps(right.wpos+(right.wip-1), left), all));
    end;

    matches := mergejoin(inputs, STEPPED(steppedCondition(left, right)) and condition(LEFT, RIGHT) and not anyOverlaps(LEFT,RIGHT), sorted(doc, segment, wpos));

    return matches;
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Normalize denormalized proximity records

doNormalizeMatch(searchRecord search, SetOfInputs inputs) := FUNCTION

    matchRecord createNorm(matchRecord l, unsigned c) := transform
        hasChildren := count(l.children) <> 0;
        curChild := l.children[NOBOUNDCHECK c];
        self.wpos := if (hasChildren, curChild.wpos, l.wpos);
        self.wip := if (hasChildren, curChild.wip, l.wip);
        self.children := [];
        self := l;
    end;

    normalizedRecords := normalize(inputs[1], MAX(1, count(LEFT.children)), createNorm(left, counter));
    groupedNormalized := group(normalizedRecords, doc, segment);
    sortedNormalized := sort(groupedNormalized, wpos);
    dedupedNormalized := dedup(sortedNormalized, wpos, wip);
    return group(dedupedNormalized);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Rollup by document

doRollupByDocument(searchRecord search, dataset(matchRecord) input) := FUNCTION
    groupByDocument := group(input, doc);
    dedupedByDocument := rollup(groupByDocument, group, transform(matchRecord, self.doc := left.doc; self.segment := 0; self.wpos := 0; self.wip := 0; self := left));
    return dedupedByDocument;
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////

processStage(searchRecord search, SetOfInputs allInputs) := function
    inputs:= RANGE(allInputs, StageDatasetToSet(search.inputs));
    result := case(search.action,
        actionEnum.ReadWord             => doReadWord(search),
        actionEnum.ReadWordSet          => doReadWordSet(search),
        actionEnum.OrTerms              => doOrTerms(search, inputs),
        actionEnum.AndTerms             => doAndTerms(search, inputs),
        actionEnum.AndNotTerms          => doAndNotTerms(search, inputs),
        actionEnum.RankMergeTerms       => doRankMergeTerms(search, inputs),
        actionEnum.MofNTerms            => doMofNTerms(search, inputs),
        actionEnum.PhraseAnd            => doPhraseAnd(search, inputs),
        actionEnum.ProximityAnd         => doProximityAnd(search, inputs),
//      actionEnum.ProximityMergeAnd    => doProximityMergeAnd(search, inputs),
        actionEnum.AndJoinTerms         => doAndJoinTerms(search, inputs),
        actionEnum.AndNotJoinTerms      => doAndNotJoinTerms(search, inputs),
        actionEnum.RankJoinTerms        => doRankJoinTerms(search, inputs),
        actionEnum.MofNJoinTerms        => doMofNJoinTerms(search, inputs),
        actionEnum.RollupByDocument     => doRollupByDocument(search, allInputs[search.inputs[1].stage]),       // more efficient than way normalize is handled, but want to test both varieties
        actionEnum.NormalizeMatch       => doNormalizeMatch(search, inputs),
        dataset([], matchRecord));

    //check that outputs from every stage are sorted as required.
    sortedResult := sorted(result, doc, segment, wpos, assert);
    return sortedResult;
end;

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Code to actually execute the query:

convertToUserOutput(dataset(matchRecord) results) := function

    simpleUserOutputRecord createUserOutput(matchRecord l) := transform
            SELF.name := docMetaIndex(doc = l.doc)[1].filename;
            SELF := l;
        END;

    return project(results, createUserOutput(left));
end;

ExecuteQuery(dataset(searchRecord) queryDefinition, dataset(matchRecord) initialResults = dataset([], matchRecord)) := function

    executionPlan := global(queryDefinition, opt, few);         // Store globally for efficient access

    results := graph(initialResults, count(executionPlan), processStage(executionPlan[NOBOUNDCHECK COUNTER], rowset(left)), parallel);

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

// A simplified query language
parseQuery(string queryText) := function

searchParseRecord :=
            RECORD(searchRecord)
unsigned        numInputs;
            END;

productionRecord  :=
            record
unsigned        termCount;
dataset(searchParseRecord) actions{maxcount(MaxActions)};
            end;

unknownTerm := (termType)-1;

PRULE := rule type (productionRecord);
ARULE := rule type (searchParseRecord);

///////////////////////////////////////////////////////////////////////////////////////////////////////////

pattern ws := [' ','\t'];

token number    := pattern('-?[0-9]+');
//pattern wordpat   := pattern('[A-Za-z0-9]+');
pattern wordpat := pattern('[A-Za-z][A-Za-z0-9]*');
pattern quotechar   := '"';
token quotedword := quotechar wordpat quotechar;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

PRULE forwardExpr := use(productionRecord, 'ExpressionRule');

ARULE term0
    := quotedword                               transform(searchParseRecord,
                                                    SELF.action := actionEnum.ReadWord;
                                                    SELF.word := $1[2..length($1)-1];
                                                    SELF := []
                                                )
    | 'CAPS' '(' SELF ')'                       transform(searchParseRecord,
                                                    SELF.wordFlagMask := wordFlags.hasUpper;
                                                    SELF.wordFlagCompare := wordFlags.hasUpper;
                                                    SELF := $3;
                                                )
    | 'NOCAPS' '(' SELF ')'                     transform(searchParseRecord,
                                                    SELF.wordFlagMask := wordFlags.hasUpper;
                                                    SELF.wordFlagCompare := 0;
                                                    SELF := $3;
                                                )
    | 'ALLCAPS' '(' SELF ')'                    transform(searchParseRecord,
                                                    SELF.wordFlagMask := wordFlags.hasUpper+wordFlags.hasLower;
                                                    SELF.wordFlagCompare := wordFlags.hasUpper;
                                                    SELF := $3;
                                                )
    ;

ARULE term0List
    := term0                                    transform(searchParseRecord,
                                                    SELF.action := actionEnum.ReadWordSet;
                                                    SELF.words := dataset(row(transform(wordRecord, self.word := $1.word)));
                                                    SELF.word := '';
                                                    SELF := $1;
                                                )
    | SELF ',' term0                            transform(searchParseRecord,
                                                    SELF.words := $1.words + dataset(row(transform(wordRecord, self.word := $3.word)));
                                                    SELF := $1;
                                                )
    ;

PRULE termList
    := forwardExpr                              transform(productionRecord, self.termCount := 1; self.actions := $1.actions)
    | SELF ',' forwardExpr                      transform(productionRecord, self.termCount := $1.termCount + 1; self.actions := $1.actions + $3.actions)
    ;

PRULE term1
    := term0                                    transform(productionRecord, self.termCount := 1; self.actions := dataset($1))
    | 'SET' '(' term0List ')'                       transform(productionRecord, self.termCount := 1; self.actions := dataset($3))
    | '(' forwardExpr ')'
    | 'AND' '(' termList ')'                    transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.AndTerms;
                                                            self.numInputs := $3.termCount;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'ANDNOT' '(' termList ')'                 transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.AndNotTerms;
                                                            self.numInputs := $3.termCount;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'RANK' '(' forwardExpr ',' forwardExpr ')'
                                                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.RankMergeTerms;
                                                            self.numInputs := 2;
                                                            self := []
                                                        )
                                                    )
                                                )
    | 'MOFN' '(' number ',' termList ')'        transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.MOfNTerms;
                                                            self.numInputs := $5.termCount;
                                                            SELF.minMatches := (integer)$3;
                                                            SELF.maxMatches := $5.termCount;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'MOFN' '(' number ',' number ',' termList ')'     transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $7.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.MOfNTerms;
                                                            self.numInputs := $7.termCount;
                                                            SELF.minMatches := (integer)$3;
                                                            SELF.maxMatches := (integer)$5;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'OR' '(' termList ')'                     transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.OrTerms;
                                                            self.numInputs := $3.termCount;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'PHRASE' '(' termList ')'                 transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.PhraseAnd;
                                                            self.numInputs := $3.termCount;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'PROXIMITY' '(' forwardExpr ',' forwardExpr ',' number ',' number ')'
                                                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.ProximityAnd;
                                                            self.numInputs := 2;
                                                            self.maxDistanceRightBeforeLeft := (integer)$7;
                                                            self.maxDistanceRightAfterLeft := (integer)$9;
                                                            self := []
                                                        )
                                                    )
                                                )
    | 'PRE' '(' forwardExpr ',' forwardExpr ')'
                                                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.ProximityAnd;
                                                            self.numInputs := 2;
                                                            self.maxDistanceRightBeforeLeft := -1;
                                                            self.maxDistanceRightAfterLeft := MaxWordsInDocument;
                                                            self := []
                                                        )
                                                    )
                                                )
    | 'AFT' '(' forwardExpr ',' forwardExpr ')'
                                                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.ProximityAnd;
                                                            self.numInputs := 2;
                                                            self.maxDistanceRightBeforeLeft := MaxWordsInDocument;
                                                            self.maxDistanceRightAfterLeft := -1;
                                                            self := []
                                                        )
                                                    )
                                                )
    | 'PROXMERGE' '(' forwardExpr ',' forwardExpr ',' number ',' number ')'
                                                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.ProximityMergeAnd;
                                                            self.numInputs := 2;
                                                            self.maxDistanceRightBeforeLeft := (integer)$7;
                                                            self.maxDistanceRightAfterLeft := (integer)$9;
                                                            self := []
                                                        )
                                                    )
                                                )
    | 'ANDJOIN' '(' termList ')'                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.AndJoinTerms;
                                                            self.numInputs := $3.termCount;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'ANDNOTJOIN' '(' termList ')'                 transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.AndNotJoinTerms;
                                                            self.numInputs := $3.termCount;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'MOFNJOIN' '(' number ',' termList ')'        transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.MOfNJoinTerms;
                                                            self.numInputs := $5.termCount;
                                                            SELF.minMatches := (integer)$3;
                                                            SELF.maxMatches := $5.termCount;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'MOFNJOIN' '(' number ',' number ',' termList ')'     transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $7.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.MOfNJoinTerms;
                                                            self.numInputs := $7.termCount;
                                                            SELF.minMatches := (integer)$3;
                                                            SELF.maxMatches := (integer)$5;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'RANKJOIN' '(' forwardExpr ',' forwardExpr ')'
                                                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + $5.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.RankJoinTerms;
                                                            self.numInputs := 2;
                                                            self := []
                                                        )
                                                    )
                                                )
    | 'ROLLAND' '(' termList ')'                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.AndTerms;
                                                            self.numInputs := $3.termCount;
                                                            self := [];
                                                        )
                                                    ) + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.RollupByDocument;
                                                            self.numInputs := 1;
                                                            self := [];
                                                        )
                                                    )
                                                )
    | 'NORM' '(' forwardExpr ')'                transform(productionRecord,
                                                    self.termCount := 1;
                                                    self.actions := $3.actions + row(
                                                        transform(searchParseRecord,
                                                            self.action := actionEnum.NormalizeMatch;
                                                            self.numInputs := 1;
                                                            self := [];
                                                        )
                                                    )
                                                )
    ;



PRULE expr
    := term1                                    : define ('ExpressionRule')
    ;

infile := dataset(row(transform({ string line{maxlength(1023)} }, self.line := queryText)));

resultsRecord := record
dataset(searchParseRecord) actions{maxcount(MaxActions)};
        end;


resultsRecord extractResults(dataset(searchParseRecord) actions) :=
        TRANSFORM
            SELF.actions := actions;
        END;

p1 := PARSE(infile,line,expr,extractResults($1.actions),first,whole,skip(ws),nocase,parse);

pnorm := normalize(p1, left.actions, transform(right));

//Now need to associate sequence numbers, and correctly set them up.

wipRecord := { wordPosType wip; };

stageStackRecord := record
    stageType prevStage;
    dataset(stageRecord) stageStack{maxcount(MaxActions)};
    dataset(wipRecord) wipStack{maxcount(MaxActions)};
end;

nullStack := row(transform(stageStackRecord, self := []));

wordPosType _max(wordPosType l, wordPosType r) := if(l < r, r, l);

assignStageWip(searchParseRecord l, stageStackRecord r) := module

    shared stageType thisStage := r.prevStage + 1;
    shared stageType maxStage := count(r.stageStack);
    shared stageType minStage := maxStage+1-l.numInputs;
    shared thisInputs := r.stageStack[minStage..maxStage];

    shared maxLeftWip := r.wipStack[minStage].wip;
    shared maxRightWip := r.wipStack[maxStage].wip;
    shared maxChildWip := max(r.wipStack[minStage..maxStage], wip);
    shared sumMaxChildWip := sum(r.wipStack[minStage..maxStage], wip);

    shared thisMaxWip := case(l.action,
            actionEnum.ReadWord=>MaxWipIndexEntry,
            actionEnum.AndTerms=>maxChildWip,
            actionEnum.OrTerms=>maxChildWip,
            actionEnum.AndNotTerms=>maxLeftWip,
            actionEnum.PhraseAnd=>sumMaxChildWip,
            actionEnum.ProximityAnd=>_max(l.maxDistanceRightBeforeLeft,l.maxDistanceRightAfterLeft) + sumMaxChildWip,
            actionEnum.MofNTerms=>maxChildWip,
            maxChildWip);


    export searchParseRecord nextRow := transform
        self.stage := thisStage;
        self.inputs := thisInputs;
        self.maxWip := thisMaxWip;
        self.maxWipLeft := maxLeftWip;
        self.maxWipRight := maxRightWip;
        self.maxWipChild := maxChildWip;
        self := l;
    end;

    export stageStackRecord nextStack := transform
        self.prevStage := thisStage;
        self.stageStack := r.stageStack[1..maxStage-l.numInputs] + row(transform(stageRecord, self.stage := thisStage));
        self.wipStack := r.wipStack[1..maxStage-l.numInputs] + row(transform(wipRecord, self.wip := thisMaxWip;));
    end;
end;


sequenced := process(pnorm, nullStack, assignStageWip(left, right).nextRow, assignStageWip(left, right).nextStack);
return project(sequenced, transform(searchRecord, self := left));

end;


#if (1)
inputRecord := { string query{maxlength(2048)}; };

MaxResults := 10000;

processedRecord := record(inputRecord)
dataset(searchRecord) request{maxcount(MaxActions)};
dataset(simpleUserOutputRecord) result{maxcount(MaxResults)};
        end;


processedRecord doBatchExecute(inputRecord l) := transform
    request := parseQuery(l.query);
    self.request := request;
    self.result := ExecuteQuery(request);
    self := l;
end;


doSingleExecute(string queryText) := function
    request := parseQuery(queryText);
    result := ExecuteQuery(request);
    return result;
end;

q1 := dataset([
            'AND("melchizedek","rahab")',
            'AND("x","z")'
            ], inputRecord);

p := project(q1, doBatchExecute(LEFT));
output(p);


#elsif (0)
//Use a dynamic graph....
q1 := dataset([
            CmdReadWord('melchizedek'),
            CmdReadWord('rahab'),
            CmdAndTerms([1,2])
            ]);

q2 := dataset([
            CmdReadWord('melchizedek')
            ]);

q3 := dataset([
            CmdReadWord('rahab')
            ]);

output(ExecuteQuery(q1));

//output(ExecuteQuery(q2));
//output(ExecuteQuery(q3));
#else

//Same attributes, using an explicit graph

r1 := row(CmdReadWord('melchizedek'));
r2 := row(CmdReadWord('rahab'));
r3 := row(CmdAndTerms([1,2]));
r4 := row(CmdTermAndNotTerms([1,2]));

x1 := doReadWord(r1);
x2 := doReadWord(r2);
x3 := doAndTerms(r3, [x1,x2]);
//output(convertToUserOutput(x3));

x4 := doAndNotTerms(r4, [x1,x2]);
//output(convertToUserOutput(x4));


q1 := executeReadWord('living');
q2 := executeReadWord('water');
q3 := executePhrase([q1, q2]);
//q3 := executeProximity([q1, q2],5,20);
output(convertToUserOutput(q3));

#end
