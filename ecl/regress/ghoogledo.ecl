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

#option ('newQueries', true);

import ghoogle;
import lib_stringLib;

ghoogle.ghoogleDefine()

//Move the following to ghoogle once syntax checked.

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Matching helper functions

matchSingleWordFlags(wordIndex wIndex, searchRecord search) :=
    keyed(search.segment = 0 or wIndex.segment = search.segment, opt) AND
    ((wIndex.flags & search.wordFlagMask) = search.wordFlagCompare);

matchSingleWord(wordIndex wIndex, searchRecord search) :=
    keyed(wIndex.word = search.word) AND
    matchSingleWordFlags(wIndex, search);

matchFirstWord(wordIndex wIndex, searchRecord search) :=
    keyed(search.source = 0 OR docMatchesSource(wIndex.doc, search.source), opt);

matchDateRange(dateType date, searchRecord search) :=
    ((date >= search.dateLow) and (date <= if(search.dateHigh = 0, (dateType)-1, search.dateHigh)));

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Some common functions

minimalMatchRecord :=
        record
documentId      doc;
wordPosType     wPos;
        end;


denormTermRecord wordsToDenormTerm(dataset(minimalMatchRecord) in, termType term) :=
        TRANSFORM
            SELF.term := term;
            SELF.cnt := count(in);
            SELF.words := project(in, transform(wordPosRecord, self := left));
        END;

denormCandidateRecord wordsToDenormCandidate(minimalMatchRecord firstin, dataset(minimalMatchRecord) in, termType term, stageType stage) :=
        TRANSFORM
            SELF.stage := stage;
            SELF.doc := firstIn.doc;
            SELF.matches := dataset(row(wordsToDenormTerm(in, term)));
        END;

createDenormCandidateFromWordMatches(searchRecord search, dataset(wordIndexRecord) matches) := function


    extracted := project(matches, transform(minimalMatchRecord, SELF := LEFT));

    extractedS := sorted(extracted, doc);           // Should be an assertion really, a requirement of the input

    groupedDs := group(extractedS, doc, local);

    rolled:= rollup(groupedDs, group, wordsToDenormCandidate(LEFT, rows(LEFT), search.term, search.stage));

    return rolled;
END;


normTermRecord wordToNormTerm(wordIndexRecord match, termType term) :=
        TRANSFORM
            SELF.term := term;
            SELF.wpos := match.wpos;
        END;

normCandidateRecord wordToNormCandidate(wordIndexRecord match, termType term, stageType stage) :=
        TRANSFORM
            SELF.stage := stage;
            SELF.doc := match.doc;
            SELF.matches := dataset(row(wordToNormTerm(match, term)));
        END;

createNormCandidateFromWordMatches(searchRecord search, dataset(wordIndexRecord) matches) := function

    return project(matches, wordToNormCandidate(LEFT, search.term, search.stage));

END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

doNormReadWord(searchRecord search) := FUNCTION

    matches := sorted(wordIndex)(
                        matchSingleWord(wordIndex, search) AND
                        matchFirstWord(wordIndex, search));

    //Because word is single valued, then must be sorted by document
    sortedMatches := sorted(matches, doc);

    return createNormCandidateFromWordMatches(search, sortedMatches);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////

doNormReadQuotedWord(searchRecord search) := FUNCTION
    matches := sorted(wordIndex)(
                        matchSingleWord(wordIndex, search) AND
                        matchFirstWord(wordIndex, search) AND
                        wordIndex.original = search.original);

    //Because word is single valued, then must be sorted by document
    sortedMatches := sorted(matches, doc);

    return createNormCandidateFromWordMatches(search, sortedMatches);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////

//wordType is fixed length at the moment, so pass in the length
boolean doMatchWildcards(unsigned4 len, wordType word, wordType wildMask, wordType wildMatch) := BEGINC++

    for (unsigned i=0; i < len; i++)
    {
        if ((word[i] & wildmask[i]) != wildmatch[i])
            return false;
        return true;
    }

ENDC++;

boolean matchWildcards(wordType word, wordType wildMask, wordType wildMatch) :=
    doMatchWildcards(length(wildMask), word, wildMask, wildMatch);


doNormReadWildWord(searchRecord search) := FUNCTION
    searchPrefix := trim(search.word);
    matches := sorted(wordIndex)(
                        keyed(wordIndex.word[1..length(searchPrefix)] = searchPrefix),
                        matchSingleWordFlags(wordIndex, search) AND
                        matchFirstWord(wordIndex, search) AND
                        matchWildcards(word, search.wildMask, search.wildMatch));

    //Because word is single valued, then must be sorted by document
    sortedMatches := sort(matches, doc);

    return createNormCandidateFromWordMatches(search, sortedMatches);
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

doNormReadDate(searchRecord search) := function
    matches := dateDocIndex(
                    keyed(matchDateRange(date, search)) AND
                    keyed(search.source = 0 OR docMatchesSource(doc, search.source), opt));

    normCandidateRecord tNorm(dateDocIndex l) := transform
        self.stage := search.stage;
        self.doc := l.doc;
        self.matches := [];
    end;

    return project(matches, tNorm(LEFT));
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Connector helpers:

boolean validAdjacent(searchRecord search, wordPosType lwPos, wordPosType rwPos) := function
    return rwPos = IF(search.preceeds, lwPos+1, lwPos -1);
END;

boolean validProximity(searchRecord search, wordPosType lwPos, wordPosType rwPos) := function

    wordPosType lowerBound(wordPosType oldpos) :=
        IF (search.preceeds, oldpos+1, IF(oldpos >= search.distance, oldpos-search.distance, 1));

    wordPosType upperBound(wordPosType oldpos) := oldpos+search.distance;

    return
        ((search.distance = 0 and not search.preceeds) OR
                rwPos >= lowerBound(lwPos)) AND
        ((search.distance = 0) OR
                rwPos <= upperBound(lwpos));
END;

boolean validDenormAdjacent(searchRecord search, denormTermRecord l, wordPosType rwPos) := function
    return exists(l.words(validAdjacent(search, wpos, rwPos)));
END;

boolean validDenormProximity(searchRecord search, denormTermRecord l, wordPosType rwPos) := function
    return exists(l.words(validProximity(search, wpos, rwPos)));
END;

boolean validNormAdjacent(searchRecord search, normTermRecord l, wordPosType rwPos) := function
    return validAdjacent(search, l.wpos, rwPos);
END;

boolean validNormProximity(searchRecord search, normTermRecord l, wordPosType rwPos) := function
    return validProximity(search, l.wpos, rwPos);
END;

denormCandidateRecord combineDeNormCandidateTransform(stageType stage, denormCandidateRecord l, denormCandidateRecord r) := TRANSFORM
        SELF.doc := IF(l.doc <> 0, l.doc, r.doc);
        SELF.stage := stage;
        SELF.matches := l.matches + r.matches;
    END;

normCandidateRecord combineNormCandidateTransform(stageType stage, normCandidateRecord l, normCandidateRecord r) := TRANSFORM
        SELF.doc := IF(l.doc <> 0, l.doc, r.doc);
        SELF.stage := stage;
        SELF.matches := l.matches + r.matches;
    END;

normCandidateRecord extendNormCandidateTransform(stageType stage, normCandidateRecord l, termType term, wordPosType wpos) := TRANSFORM
        SELF.doc := l.doc;
        SELF.stage := stage;
        SELF.matches := l.matches + row(transform(normTermRecord, self.term := term, self.wpos := wpos));
    END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

doNormTermAndTerm(searchRecord search, dataset(normCandidateRecord) in) := function
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    inRight := sort(in(stage = search.rightStage), doc);        // NB: change to sorted() not sort
    others := in(stage not in [search.leftStage, search.rightStage]);       // generates poor code: why?

    matches := join(inLeft, inRight, left.doc = right.doc, combineNormCandidateTransform(search.stage, LEFT, RIGHT), INNER);
    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

doNormTermAndNotTerm(searchRecord search, dataset(normCandidateRecord) in) := function
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    inRight := sort(in(stage = search.rightStage), doc);        // NB: change to sorted() not sort
    others := in(stage not in [search.leftStage, search.rightStage]);       // generates poor code: why?

    matches := join(inLeft, inRight, left.doc = right.doc, combineNormCandidateTransform(search.stage, LEFT, RIGHT), LEFT ONLY);
    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

doNormTermAndProxTerm(searchRecord search, dataset(normCandidateRecord) in) := function
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    inRight := sort(in(stage = search.rightStage), doc);        // NB: change to sorted() not sort
    others := in(stage not in [search.leftStage, search.rightStage]);       // generates poor code: why?

    matches := join(inLeft, inRight,
                    left.doc = right.doc and
                    validNormProximity(search, left.matches(term = search.leftTerm)[1], right.matches(term = search.rightTerm)[1].wPos),
                    combineNormCandidateTransform(search.stage, LEFT, RIGHT), INNER);
    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

doNormTermAndNotProxTerm(searchRecord search, dataset(normCandidateRecord) in) := function
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    inRight := sort(in(stage = search.rightStage), doc);        // NB: change to sorted() not sort
    others := in(stage not in [search.leftStage, search.rightStage]);       // generates poor code: why?

    matches := join(inLeft, inRight,
                    left.doc = right.doc and
                    validNormProximity(search, left.matches(term = search.leftTerm)[1], right.matches(term = search.rightTerm)[1].wPos),
                    combineNormCandidateTransform(search.stage, LEFT, RIGHT), left only);
    return others + matches;
end;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

//MORE: The generated code for this leaves something to be desired!
doNormProximityFilter(searchRecord search, dataset(normCandidateRecord) in) := function
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    others := in(stage != search.leftStage);

    matchesProximityFilter(dataset(normTermRecord) l, dataset(normTermRecord) r) :=
        exists(choosen(join(l, r, validProximity(search, left.wpos, right.wpos), all), 1));

    filtered := inLeft(matchesProximityFilter(matches(term in search.leftTermSet), matches(term in search.rightTermSet)));
    matches := project(filtered, transform(recordof(in), self.stage := search.stage, self := left));
    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

doNormTermOrTerm(searchRecord search, dataset(normCandidateRecord) in) := FUNCTION
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    inRight := sort(in(stage = search.rightStage), doc);        // NB: change to sorted() not sort
    others := in(stage not in [search.leftStage, search.rightStage]);       // generates poor code: why?

    matches := join(inLeft, inRight, left.doc = right.doc, combineNormCandidateTransform(search.stage, LEFT, RIGHT), FULL OUTER);
    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

doNormTermAndWordBad(searchRecord search, dataset(normCandidateRecord) in) := function
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    others := in(stage != search.leftStage);

    matches := join(inLeft, wordIndex,
                    keyed(left.doc = right.doc) AND
                    matchSingleWord(RIGHT, search),
                    extendNormCandidateTransform(search.stage, LEFT, search.term, right.wpos), INNER);
    return others + matches;
END;

doNormTermAndWord(searchRecord search, dataset(normCandidateRecord) in) := function
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    others := in(stage != search.leftStage);

    //Only return a single row per input doc
    dedupLeft := rollup(group(inLeft, doc), group, transform(docRecord, SELF.doc := LEFT.doc));

    wordMatches := join(dedupLeft, wordIndex,
                    keyed(left.doc = right.doc) AND
                    matchSingleWord(RIGHT, search),
                    transform(docPosRecord, self.doc := left.doc, self.wpos := right.wpos), INNER);

    //NB: Both inputs should be sorted... so this should be a merge
    matches := join(inLeft, wordMatches,
                    left.doc = right.doc,
                    extendNormCandidateTransform(search.stage, LEFT, search.term, right.wpos), INNER);

    //Now join them back to the full set of information

    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

doNormTermAdjacentWord(searchRecord search, dataset(normCandidateRecord) in) := function
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    others := in(stage != search.leftStage);

    //Only return a single row per input doc, with a deduped list of posible positions
    dedupPositions(dataset(normCandidateRecord) ds) := function
        ds1 := project(ds, transform(wordPosRecord, self.wpos := left.matches(term = search.leftTerm)[1].wpos));
        ds2 := sort(ds1, wpos);
        ds3 := dedup(ds2, wpos);
        return ds3;
    end;

    dedupLeft := rollup(group(inLeft, doc), group, transform(docPosSetRecord, SELF.doc := LEFT.doc, self.positions := dedupPositions(rows(left))));

    //Keyed join to get a list of candidates for each document
    wordMatches := join(dedupLeft, wordIndex,
                    keyed(left.doc = right.doc) AND
                    matchSingleWord(RIGHT, search) and
                    (search.original = '' or right.original=search.original) and                            // also used for original word checking.
                    exists(left.positions(validAdjacent(search, left.positions.wpos, right.wpos))),
                    transform(docPosRecord, self.doc := left.doc, self.wpos := right.wpos), INNER);

    //Now join them back to the full set of information
    matches := join(inLeft, wordMatches,
                    left.doc = right.doc and
                    validAdjacent(search, left.matches(term = search.leftTerm)[1].wpos, right.wpos),
                    extendNormCandidateTransform(search.stage, LEFT, search.term, right.wpos), INNER);


    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

wposInRange(searchRecord search, normCandidateRecord l, wordPosType wpos) := function
    leftMatch := l.matches(term = search.leftTerm)[1];
    leftMatchPos := leftMatch.wpos;
    rightMatch := l.matches(term = search.rightTerm)[1];
    rightMatchPos := rightMatch.wpos;
    lowerPos := if (leftMatchPos < rightMatchPos, leftMatchPos, rightMatchPos);
    upperPos := if (leftMatchPos < rightMatchPos, rightMatchPos, leftMatchPos);
    return (wpos > lowerPos) and (wpos <= upperPos);
END;


doNormSentanceFilter(searchRecord search, dataset(normCandidateRecord) in) := FUNCTION
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    others := in(stage <> search.leftStage);

    matches := join(inLeft, sentenceIndex,
                    keyed(left.doc = right.doc) AND
                    (wposInRange(search, left, right.wpos)),            // make this keyed when wpos not in payload
                    transform(normCandidateRecord, self.stage := search.stage; self := left), LEFT ONLY);
    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

doNormParagraphFilter(searchRecord search, dataset(normCandidateRecord) in) := FUNCTION
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    others := in(stage <> search.leftStage);

    matches := join(inLeft, paragraphIndex,
                    keyed(left.doc = right.doc) AND
                    (wposInRange(search, left, right.wpos)),            // make this keyed when wpos not in payload
                    transform(normCandidateRecord, self.stage := search.stage; self := left), LEFT ONLY);
    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

doNormTermAndDate(searchRecord search, dataset(normCandidateRecord) in) :=
    FAIL(recordof(in), 'Action doTermAndDate not yet implemented');

///////////////////////////////////////////////////////////////////////////////////////////////////////////

doNormAtleast(searchRecord search, dataset(normCandidateRecord) in) := function
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    others := in(stage <> search.leftStage);

    gr := group(inLeft, doc);

    recordof(in) createAtleastResult(normCandidateRecord firstRow, dataset(normCandidateRecord) allRows) :=
        transform, skip(count(allRows) < search.minCount)
            self.doc := firstRow.doc;
            self.stage := search.stage;
            self.matches := [];
        end;

    matches := rollup(gr, group, createAtleastResult(left, rows(left)));
    return others + matches;
end;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

doNormAtleastTerms(searchRecord search, dataset(normCandidateRecord) in) := function
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    others := in(stage <> search.leftStage);

    //type transfer is horrible, but enables us to group by a dataset...
    gr1 := group(inLeft, doc);
    pr1 := project(gr1, transform(normCandidateRecord, self.matches := left.matches(term not in search.leftTermSet); self := left));
    srt1 := sort(pr1, transfer(matches, data));
    gr := group(srt1, doc, transfer(matches, data));

    recordof(in) createAtleastResult(normCandidateRecord firstRow, dataset(normCandidateRecord) allRows) :=
        transform, skip(count(allRows) < search.minCount)
            self.doc := firstRow.doc;
            self.stage := search.stage;
            self.matches := firstRow.matches;
        end;

    matches := rollup(gr, group, createAtleastResult(left, rows(left)));
    return others + matches;
end;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

processStageNorm(searchRecord search, dataset(normCandidateRecord) in) :=
    case(search.action,
        actionEnum.ReadWord             => in+doNormReadWord(search),
        actionEnum.ReadQuotedWord       => in+doNormReadQuotedWord(search),
        actionEnum.ReadWildWord         => in+doNormReadWildWord(search),
        actionEnum.ReadDate             => in+doNormReadDate(search),
        actionEnum.TermAndTerm          => doNormTermAndTerm(search, in),
        actionEnum.TermAndNotTerm       => doNormTermAndNotTerm(search, in),
        actionEnum.TermAndProxTerm      => doNormTermAndProxTerm(search, in),
        actionEnum.TermAndNotProxTerm   => doNormTermAndNotProxTerm(search, in),
        actionEnum.TermOrTerm           => doNormTermOrTerm(search, in),
        actionEnum.TermAndWord          => doNormTermAndWord(search, in),
        actionEnum.TermAdjacentWord     => doNormTermAdjacentWord(search, in),
        actionEnum.TermAdjacentQuoted   => doNormTermAdjacentWord(search, in),
        actionEnum.ProximityFilter      => doNormProximityFilter(search, in),
        actionEnum.SentanceFilter       => doNormSentanceFilter(search, in),
        actionEnum.ParagraphFilter      => doNormParagraphFilter(search, in),
        actionEnum.TermAndDate          => doNormTermAndDate(search, in),
        actionEnum.AtleastTerms         => doNormAtleastTerms(search, in),
        actionEnum.Atleast              => doNormAtleast(search, in),
        actionEnum.PassThrough          => in,
        FAIL(recordof(in), 'Unknown action '+ (string)search.action));

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Code to actually execute the query:

ExecuteReduceFullQuery(dataset(searchRecord) queryDefinition, dataset(normCandidateRecord) initialResults) := function

    executionPlan := global(queryDefinition, few);          // Store globally for efficient access

    results := LOOP(initialResults, count(executionPlan), processStageNorm(executionPlan[NOBOUNDCHECK COUNTER], rows(left)));


    userOutputMatchRecord   := RECORD
    termType                term;
    docPosType              dpos;
                        END;

    userOutputRecord :=
            record
    string  name;
    dataset(userOutputMatchRecord)  positions;          // which stage last touched this row
            end;

    userOutputMatchRecord createUserOutputMatch(normCandidateRecord l, normTermRecord lm) := transform
            self.term := lm.term;
            self.dpos := docPosIndex(doc=l.doc and wpos = lm.wpos)[1].dpos;
            end;

    userOutputRecord createUserOutput(normCandidateRecord l) := transform
            SELF.name := docMetaIndex(doc = l.doc)[1].filename;
            SELF.positions := sort(project(l.matches, createUserOutputMatch(l, LEFT)), term);
        END;

    return project(results, createUserOutput(left));
end;



ExecuteFullQuery(dataset(searchRecord) queryDefinition) := function

    initialResults := dataset([], normCandidateRecord);

    return ExecuteReduceFullQuery(queryDefinition, initialResults);

end;

///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////

simpleCandidateRecord createSimpleCandidateTransform(stageType stage, simpleCandidateRecord l) := TRANSFORM
        SELF.doc := l.doc;
        SELF.stage := stage;
    END;

simpleCandidateRecord combineSimpleCandidateTransform(stageType stage, simpleCandidateRecord l, simpleCandidateRecord r) := TRANSFORM
        SELF.doc := IF(l.doc <> 0, l.doc, r.doc);
        SELF.stage := stage;
    END;


reduceMultipleToSingle(searchRecord search, dataset(recordof(wordIndex)) matches) := FUNCTION
    reduced := project(matches, transform(docRecord, self := left));

    //Because word is single valued, then must be sorted by document
    sortedMatches := sorted(reduced, doc);
    groupedMatches := group(sortedMatches, doc);
    deduped := rollup(groupedMatches, group, transform(simpleCandidateRecord, self.doc := left.doc; self.stage := search.stage));
    return deduped;
end;

doSimpleReadWordX(searchRecord search, boolean checkOriginal) := FUNCTION

    matches := sorted(wordIndex)(
                        matchSingleWord(wordIndex, search) AND
                        matchFirstWord(wordIndex, search) and
                        (not checkOriginal or wordIndex.original = search.original));

    return reduceMultipleToSingle(search, matches);
END;


doSimpleReadWord(searchRecord search) :=
    doSimpleReadWordX(search, false);


///////////////////////////////////////////////////////////////////////////////////////////////////////////

doSimpleReadQuotedWord(searchRecord search) :=
    doSimpleReadWordX(search, true);


///////////////////////////////////////////////////////////////////////////////////////////////////////////

doSimpleReadWildWord(searchRecord search) := FUNCTION
    searchPrefix := trim(search.word);
    matches := sorted(wordIndex)(
                        keyed(wordIndex.word[1..length(searchPrefix)] = searchPrefix),
                        matchSingleWordFlags(wordIndex, search) AND
                        matchFirstWord(wordIndex, search) AND
                        matchWildcards(word, search.wildMask, search.wildMatch));

    return reduceMultipleToSingle(search, matches);
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

doSimpleReadDate(searchRecord search) := function
    matches := dateDocIndex(
                    keyed(matchDateRange(date, search)) AND
                    keyed(search.source = 0 OR docMatchesSource(doc, search.source), opt));

    simpleCandidateRecord tNorm(dateDocIndex l) := transform
        self.stage := search.stage;
        self.doc := l.doc;
    end;

    return project(matches, tNorm(LEFT));
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

doSimpleTermAndTerm(searchRecord search, dataset(simpleCandidateRecord) in) := function
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    inRight := sort(in(stage = search.rightStage), doc);        // NB: change to sorted() not sort
    others := in(stage not in [search.leftStage, search.rightStage]);       // generates poor code: why?

    matches := join(inLeft, inRight, left.doc = right.doc, createSimpleCandidateTransform(search.stage, LEFT), INNER);
    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

doSimpleTermAndNotTerm(searchRecord search, dataset(simpleCandidateRecord) in) := function
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    inRight := sort(in(stage = search.rightStage), doc);        // NB: change to sorted() not sort
    others := in(stage not in [search.leftStage, search.rightStage]);       // generates poor code: why?

    matches := join(inLeft, inRight, left.doc = right.doc, createSimpleCandidateTransform(search.stage, LEFT), LEFT ONLY);
    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

doSimpleTermOrTerm(searchRecord search, dataset(simpleCandidateRecord) in) := FUNCTION
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    inRight := sort(in(stage = search.rightStage), doc);        // NB: change to sorted() not sort
    others := in(stage not in [search.leftStage, search.rightStage]);       // generates poor code: why?

    matches := join(inLeft, inRight, left.doc = right.doc, combineSimpleCandidateTransform(search.stage, LEFT, RIGHT), FULL OUTER);
    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

doSimpleTermAndWord(searchRecord search, dataset(simpleCandidateRecord) in) := function
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    others := in(stage != search.leftStage);

    matches := join(inLeft, wordIndex,
                    keyed(left.doc = right.doc) AND
                    matchSingleWord(RIGHT, search),
                    createSimpleCandidateTransform(search.stage, LEFT), INNER, keep(1));
    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

doSimpleTermAndDate(searchRecord search, dataset(simpleCandidateRecord) in) :=
    FAIL(recordof(in), 'Action doTermAndDate not yet implemented');

///////////////////////////////////////////////////////////////////////////////////////////////////////////

processStageSimple(searchRecord search, dataset(simpleCandidateRecord) in) :=
    case(search.action,
        actionEnum.ReadWord             => in+doSimpleReadWord(search),
        actionEnum.ReadQuotedWord       => in+doSimpleReadQuotedWord(search),
        actionEnum.ReadWildWord         => in+doSimpleReadWildWord(search),
        actionEnum.ReadDate             => in+doSimpleReadDate(search),
        actionEnum.TermAndTerm          => doSimpleTermAndTerm(search, in),
        actionEnum.TermAndNotTerm       => doSimpleTermAndNotTerm(search, in),
        actionEnum.TermOrTerm           => doSimpleTermOrTerm(search, in),
        actionEnum.TermAndWord          => doSimpleTermAndWord(search, in),
        actionEnum.TermAndDate          => doSimpleTermAndDate(search, in),
        actionEnum.PassThrough          => in,
        FAIL(recordof(in), 'Unknown action '+ (string)search.action));


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Code to actually execute the query:

SimpleQueryExecutor(dataset(searchRecord) queryDefinition, dataset(simpleCandidateRecord) initialResults) := module

    executionPlan := global(queryDefinition, few);          // Store globally for efficient access

    export internalResults := LOOP(initialResults, count(executionPlan), processStageSimple(executionPlan[NOBOUNDCHECK COUNTER], rows(left)));


    simpleUserOutputRecord :=
            record
    string  name;
            end;

    simpleUserOutputRecord createUserOutput(simpleCandidateRecord l) := transform
            SELF.name := docMetaIndex(doc = l.doc)[1].filename;
        END;

    export userOutput := project(internalResults, createUserOutput(left));

end;

ExecuteReduceSimpleQuery(dataset(searchRecord) queryDefinition, dataset(simpleCandidateRecord) initialResults) := function

    return SimpleQueryExecutor(queryDefinition, initialResults).userOutput;
end;


ExecuteSimpleQuery(dataset(searchRecord) queryDefinition) := function

    initialResults := dataset([], simpleCandidateRecord);

    return ExecuteReduceSimpleQuery(queryDefinition, initialResults);
end;

///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////


ExecuteTwoPassQuery(dataset(searchRecord) queryDefinition) := function

    //first filter the plan to remove all proximity operators:
    searchRecord removeProximity(searchRecord l) := transform
        SELF.action := CASE(l.action,
                        actionEnum.TermAdjacentTerm=>actionEnum.TermAndTerm,
                        actionEnum.TermAndProxTerm=>actionEnum.TermAndTerm,
                        actionEnum.TermAndNotProxTerm=>actionEnum.TermAndNotTerm,
                        actionEnum.TermAdjacentWord=>actionEnum.TermAndWord,
                        actionEnum.TermAdjacentQuoted=>actionEnum.TermAndQuoted,            // Not yet implemented!
                        actionEnum.ProximityFilter=>actionEnum.PassThrough,
                        actionEnum.SentanceFilter=>actionEnum.PassThrough,
                        actionEnum.ParagraphFilter=>actionEnum.PassThrough,
                        actionEnum.AtleastTerms=>actionEnum.PassThrough,
                        actionEnum.Atleast=>actionEnum.PassThrough,
                        l.action);
        SELF := l;
    END;

    simpleSearch := project(queryDefinition, removeProximity(LEFT));

    initialResults := dataset([], simpleCandidateRecord);

    simpleResults := SimpleQueryExecutor(queryDefinition, initialResults).internalResults;

    //Now modify the original proximity so any terminals are converted to joins
    //first filter the plan to remove all proximity operators:
    searchRecord modifyTerminals(searchRecord l) := transform
        SELF.action := CASE(l.action,
                        actionEnum.ReadWord=>actionEnum.TermAndWord,
                        actionEnum.ReadQuotedWord=>actionEnum.TermAndQuoted,
                        actionEnum.ReadWildWord=>actionEnum.TermAndWildWord,                // not implemented!
                        actionEnum.ReadDate=>actionEnum.TermAndDate,
                        l.action);
        SELF := l;
    END;

    finalSearch := project(queryDefinition, modifyTerminals(LEFT));

    normCandidateRecord simpleToNormCandidate(simpleCandidateRecord l) := transform
            self.stage := 0;
            self := l;
            self := [];
        end;

    //This won't work because the inputs either need to be left in place, or duplicated for each of the reads that have been
    //converted to joins
    pass2Input := project(simpleResults, simpleToNormCandidate(left));

    fullResults := ExecuteReduceFullQuery(finalSearch, pass2Input);

    return fullResults;
end;

///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////

q1 := dataset([
        CmdReadWord(1, 0, 0, 'david'),
        CmdReadWord(2, 0, 0, 'goliath'),
        CmdTermAndTerm(1, 2),
        CmdParagraphFilter(3, 1, 2)
        ]);

q2 := dataset([
        CmdReadWord(1, 0, 0, 'abraham'),
        CmdReadWord(2, 0, 0, 'jesus'),
        CmdTermAndNotTerm(1, 2)
        ]);

q3 := dataset([
        CmdReadWord(1, 0, 0, 'abraham'),
        CmdReadWord(2, 0, 0, 'ruth'),
        CmdTermAndTerm(1, 2),
        CmdAtleastTerm(3, [2], 1)
        ]);

q4 := dataset([
        CmdReadWord(1, 0, 0, 'abraham'),
        CmdTermAndWord(2, 1, 0, 'ruth')
        ]);

searchDefinition := q3;

projectStageNums := project(searchDefinition, transform(searchRecord, SELF.stage := COUNTER; SELF := LEFT));

userOutput := ExecuteTwoPassQuery(projectStageNums);

//output(userOutput)

import ghoogleo;

annotated := ghoogleo.annotateQuery(projectStageNums);
optimized := ghoogleo.optimizeQuery(annotated);
output(projectStageNums, named('Original'));
output(annotated, named('Annotated'));
output(optimized, named('optimized'));
