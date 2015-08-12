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

import ghoogle;

ghoogle.ghoogleDefine()

#option ('newQueries', true);
//#option ('targetClusterType', 'roxie');

///////////////////////////////////////////////////////////////////////////////////////////////////////////

// Matches

termRecord :=
        record
termType        termnum;
unsigned4       cnt;            // used if doing atleast()
dataset(wordRecord) words;
        end;

candidateRecord :=
        record
documentId      doc;
stageType       lastStage;
dataset(termRecord) matches;
        end;



///////////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Atom implementions:
//
minimalMatchRecord :=
        record
documentId      doc;
wordPosType     wPos;
        end;


termRecord wordsToTerm(dataset(minimalMatchRecord) in, termType termnum) :=
        TRANSFORM
            SELF.termnum := termnum;
            SELF.cnt := count(in);
            SELF.words := project(in, transform(wordRecord, self := left));
        END;

candidateRecord wordsToCandidate(minimalMatchRecord firstin, dataset(minimalMatchRecord) in, termType termnum, stageType stage) :=
        TRANSFORM
            SELF.lastStage := stage;
            SELF.doc := firstIn.doc;
            SELF.matches := dataset(row(wordsToTerm(in, termNum)));
        END;

termRecord matchToTerm(wordIndex firstin, dataset(wordIndexRecord) in, termType termnum) :=
        TRANSFORM
            SELF.termnum := termnum;
            SELF.cnt := count(in);
            SELF.words := project(in, transform(wordRecord, self.wPos := left.wpos));
        END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////

boolean connectorMatchWpos(searchRecord search, termRecord l, wordPosType rwPos) := function

    wordPosType lowerBound(wordPosType oldpos) :=
        IF (search.preceeds, oldpos+1, IF(oldpos >= search.distance, oldpos-search.distance, 1));

    wordPosType upperBound(wordPosType oldpos) := oldpos+search.distance;

    return
        ((search.distance = 0 and not search.preceeds) OR
                exists(l.words(rwPos >= lowerBound(wPos)))) AND
        ((search.distance = 0) OR
                exists(l.words(rwPos <= upperBound(wpos))));
END;

boolean connectorMatchPrototype(searchRecord search, termRecord l, wordIndexRecord r) := true;

boolean defaultConnectorMatch(searchRecord search, termRecord l, wordIndexRecord r) := function

    return connectorMatchWpos(search, l, r.wpos);

END;



///////////////////////////////////////////////////////////////////////////////////////////////////////////


//boolean wordJoinFunction(searchRecord options, wordPosType lPos, corpusIndex r) := true;

boolean wordExtraMatch(wordIndex wIndex, searchRecord search) :=
                    (keyed(search.segment = 0 OR wIndex.segment = search.segment, opt) and
                    keyed(search.source = 0 OR docMatchesSource(wIndex.doc, search.source), opt) and
                    (wIndex.flags & search.wordFlagMask) = search.wordFlagCompare);


boolean wordMatchPrototype(wordIndex wIndex, searchRecord search) := true;

matchSingleWord(wordIndex wIndex, searchRecord search) :=
    keyed(wIndex.word = search.word);

//--------------------------------- Words ---------------------------------//

createCandidateFromWordMatches(searchRecord search, dataset(wordIndexRecord) matches) := function

    extracted := project(matches, transform(minimalMatchRecord, SELF := LEFT));

    groupedDs := group(sorted(extracted, doc), doc);        // actually sorted by word, doc - needs work for partial matching; possibly final dedup.

    rolled:= rollup(groupedDs, group, wordsToCandidate(LEFT, rows(LEFT), search.termnum, search.stage));

    return rolled;
END;


getReadWord(searchRecord search, wordMatchPrototype matcher) := function

    matches := sorted(wordIndex)(
                        matcher(wordIndex, search),
                        wordExtraMatch(wordIndex, search));

    return createCandidateFromWordMatches(search, matches);
END;

getJoinWord(dataset(candidateRecord) candidate, searchRecord search, wordMatchPrototype matcher, connectorMatchPrototype connector) := function

    candidateRecord extendMatches(candidateRecord l, dataset(wordIndexRecord) matches) :=
            TRANSFORM
                extracted := project(matches, transform(minimalMatchRecord, SELF := LEFT));
                SELF.matches := l.matches + row(wordsToTerm(extracted, search.termnum));
                SELF := l;
            END;

    matches := denormalize(candidate, wordIndex,
                        keyed(left.doc = right.doc) and
                        matcher(right, search) and
                        connector(search, left.matches(termnum = search.leftTerm), right) and
                        wordExtraMatch(right, search), GROUP,
                        extendMatches(LEFT, rows(RIGHT)));

    return matches;
END;

//--------------------------------- Dates ---------------------------------//

doDateCheck(dateType date, searchRecord search) :=
//  (((search.dateLow = 0) or (date >= search.dateLow)) and ((search.dateHigh = 0) or (date <= search.dateHigh)));
    ((date >= search.dateLow) and (date <= if(search.dateHigh = 0, (dateType)-1, search.dateHigh)));

getReadDate(searchRecord search) := function

    matches := dateDocIndex(keyed(doDateCheck(date, search)));

    candidateRecord t(dateDocIndex l) := transform
        self.lastStage := search.stage;
        self.doc := l.doc;
        self.matches := [];
    end;

    return project(matches, t(LEFT));
END;


getJoinDate(dataset(candidateRecord) candidate, searchRecord search) := function

    return candidate(count(docMetaIndex(keyed(doc = candidate.doc) and keyed(doDateCheck(date, search), opt))) > 0);
END;


//--------------------------------- Atom independent  ---------------------------------//

getJoin(dataset(candidateRecord) candidate, searchRecord search) := function

    matchLeft := candidate.matches(termNum = search.leftTerm);
    matchRight := candidate.matches(termNum = search.rightTerm);
    matches := candidate(exists(choosen(join(matchLeft, matchRight[1].words, connectorMatchWpos(search, left, right.wpos), all), 9999)));
//  matches := candidate(exists(matchLeft.words) and exists(matchRight.words));

    return matches;
END;

//--------------------------------- Processing for each kind of action ---------------------------------//

input(dataset(candidateRecord) in, stageType search) := in(lastStage = search);
noninput(dataset(candidateRecord) in, stageType search) := in(lastStage <> search);
input2(dataset(candidateRecord) in, stageType search1, stageType search2) := in(lastStage in [search1, search2]);
noninput2(dataset(candidateRecord) in, stageType search1, stageType search2) := in(lastStage not in [search1, search2]);

// Processor:
doProcessReadAction(searchRecord search, dataset(candidateRecord) in, wordMatchPrototype matcher = wordMatchPrototype) := function

    compoundMatchSingleWord(wordIndex wIndex, searchRecord search) := matcher(wIndex, search) AND matchSingleWord(wIndex, search);

    return case(search.termKind,
        atomEnum.PlainWord  => in+getReadWord(search, compoundMatchSingleWord),
//      atomEnum.QuotedWord =>
//      atomEnum.TruncWord  =>
//      atomEnum.WildWord   =>
        atomEnum.Date       => in+getReadDate(search),
        FAIL(candidateRecord, 'Unknown term kind '+(string)(search.termKind)));
END;

processReadAction(searchRecord search, dataset(candidateRecord) in) := doProcessReadAction(search, in);

processJoinReadAction(searchRecord search, dataset(candidateRecord) in) := function

    stageInput := input(in, search.leftStage);
    matches :=
        case(search.termKind,
            atomEnum.PlainWord  => getJoinWord(stageInput, search, matchSingleWord, defaultConnectorMatch),
    //      atomEnum.QuotedWord =>
    //      atomEnum.TruncWord  =>
    //      atomEnum.WildWord   =>
            atomEnum.Date       => getJoinDate(stageInput, search),
            FAIL(candidateRecord, 'Unknown term kind '+(string)(search.termKind)));

    return noninput(in, search.leftStage) + matches;
end;

processJoinAction(searchRecord search, dataset(candidateRecord) in) :=
        noninput2(in, search.leftStage, search.rightTerm) +
        getJoin(input2(in, search.leftStage, search.rightStage), search);


processLocalOrAction(searchRecord search, dataset(candidateRecord) in) := function

    notAlreadyExists(wordIndex wIndex, searchRecord search) := (wIndex.doc not in set(in, doc));

    return case(search.termKind,
        atomEnum.Date       => FAIL(candidateRecord, 'Invalid combination'+(string)(search.termKind)),
        doProcessReadAction(search, in, notAlreadyExists));

end;

//--------------------------------- Core processing loop ---------------------------------//

processStage(searchRecord search, dataset(candidateRecord) in) :=
    case(search.action,
        actionEnum.Read         =>processReadAction(search, in),
        actionEnum.JoinRead     =>processJoinReadAction(search, in),
        actionEnum.Join         =>processJoinAction(search, in),
        actionEnum.LocalOr      =>processLocalOrAction(search, in),
//      actionEnum.GlobalOr     =>processGlobalOrAction(search, in),
//      actionEnum.Atmost       =>processAtmostAction(search, in),
        FAIL(candidateRecord, 'Unknown action '+ (string)search.action));


executionPlan := dataset('actions', searchRecord, thor) : global(few);

initialResults := dataset([], candidateRecord);

results := LOOP(initialResults, count(executionPlan), processStage(executionPlan[COUNTER], rows(left)));

output(results);


