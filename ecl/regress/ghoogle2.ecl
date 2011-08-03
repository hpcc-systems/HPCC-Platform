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


sourceType      := unsigned2;
wordCountType   := unsigned8;
segmentType     := unsigned1;
wordPosType     := unsigned8;
docPosType      := unsigned8;
documentId      := unsigned8;
termType        := unsigned1;
distanceType    := unsigned1;

sourceType docid2source(documentId x) := (x >> 48);
unsigned8 docid2doc(documentId x) := (x & 0xFFFFFFFFFFFF);
documentId createDocId(sourceType source, unsigned6 doc) := (documentId)(((unsigned8)source << 48) | doc);
boolean docMatchesSource(unsigned8 docid, sourceType source) := (docid between createDocId(source,0) and (documentId)(createDocId(source+1,0)-1));

wordType        := string20;
wordFlags       := enum(unsigned1, HasLower=1, HasUpper=2);
savedWordPosType:= record
wordPosType          wpos;
docPosType           dpos;
//what else do we need to save?  segement?
                   end;
wordIdType      := unsigned4;
corpusFlags     := enum(unsigned1, isStopWord = 1);

corpusIndex     := index({ wordType word, wordIdType wordid, wordCountType numOccurences, wordCountType numDocuments, wordType normalized, corpusFlags flags } , 'corpusIndex');
wordIndex       := index({ wordType word, documentId doc, segmentType segment, wordFlags flags, wordType original, wordPosType wpos } , { docPosType dpos}, 'wordIndex');
sentanceIndex   := index({ wordType word, documentId doc, wordPosType wpos }, 'sentanceIndex');
paragraphIndex  := index({ wordType word, documentId doc, wordPosType wpos }, 'paragraphIndex');

//May want the following, probably not actually implemented as an index - would save having dpos in the index, but more importantly storing it in the candidate match results because the mapping could be looked up later.
docPosIndex     := index({ wordPosType wpos }, { docPosType dpos }, 'docPosIndex');

//Again not implemented as an index (probably FETCHes from a flat file), but logically:
tokenizedDocIndex   := index({ documentId doc, wordPosType wpos}, { wordIdType wordid }, 'tokenisedDocIndex');

//may also need - probably implemented as a FETCH on a file, possibly on a memory based roxie file.
tokenIndex      := index({ wordIdType wordid }, { wordType word, wordIdType normalized }, 'tokenIndex');
///////////////////////////////////////////////////////////////////////////////////////////////////////////

atomEnum  := ENUM(NoAtom, PlainWordAtom,QuotedWordAtom,TruncWordAtom,WildWordAtom,DateAtom);
actionEnum := ENUM(ReadAction,
                   JoinReadAction, JoinAction,
                   OrAction,
                   AtmostCountAction,
                   AtmostAction);

actionRecord :=
            RECORD
atomEnum        atom;
wordType        word;
unsigned1       truncpos;
actionEnum      action;
sourceType      source;
wordFlags       wordFlagMask;
wordFlags       wordFlagCompare;
boolean         preceeds;
boolean         sameSentence;
boolean         sameParagraph;
distanceType    distance;
segmentType     segment;
termType        termNum;
termType        otherTermNum;
            END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

// Version 1 - document, word are stored expanded in both input and indexes

termRecord :=
        record
documentId      docid;
termType        termnum;
wordPosType     wordpos;
        end;

candidateRecord :=
        record
dataset(termRecord) matches;
        end;


termRecord wordToTerm(wordIndex in, termType termnum) :=
        TRANSFORM
            SELF.docid := in.doc;
            SELF.wordpos := in.wpos;
            SELF.termnum := termnum;
        END;

//

dataset(termRecord) doReadPlainWord(actionRecord in) := FUNCTION

    matches := wordIndex(keyed(word = in.word),
                         in.segment = 0 OR segment = in.segment,
                         keyed(in.source = 0 OR docMatchesSource(doc, in.source), opt),
                         (flags & in.wordFlagMask) = in.wordFlagCompare);


    extracted := project(matches, wordToTerm(LEFT, in.termnum));

    return extracted;
END;


dataset(candidateRecord) doReadPlainWord1(actionRecord in) := FUNCTION
    matches := doReadPlainWord(in);

    candidateRecord createCandidate(termRecord term) :=
            transform
                SELF.matches := dataset(term);
                SELF := term;
            END;

    return project(matches, createCandidate(LEFT));
END;

termRecord getTerm(candidateRecord l, termType searchTermNum) := l.matches(termNum = searchTermNum)[1];

dataset(candidateRecord) doJoinReadPlainWord(actionRecord in, dataset(candidateRecord) candidates) := FUNCTION

    candidateRecord addTerm(candidateRecord l, wordIndex r, termType termnum) :=
            transform
                SELF.matches := l.matches + row(wordToTerm(r, termnum));
                SELF := l;
            END;

    wordPosType lowerBound(wordPosType oldpos, actionRecord in) :=
        IF (in.preceeds, oldpos+1, IF(oldpos >= in.distance, oldpos-in.distance, 1));

    wordPosType upperBound(wordPosType oldpos, actionRecord in) := oldpos+in.distance;

    matches := join(candidates, wordIndex,
                    keyed(right.word = in.word) and
                    keyed(right.doc = getTerm(left, in.otherTermNum).docid) and
                    keyed(in.segment = 0 OR right.segment = in.segment, opt) and
                    (right.flags & in.wordFlagMask) = in.wordFlagCompare and
                    ((in.distance = 0 and not in.preceeds) OR right.wpos >= lowerBound(getTerm(left, in.otherTermNum).wordpos, in)) AND
                    (in.distance = 0 OR right.wpos <= upperBound(getTerm(left, in.otherTermNum).wordpos, in))
                    ,
                    addTerm(LEFT, RIGHT, in.termnum));

    return matches;
END;



executionPlan := dataset('actions', actionRecord, thor) : global(few);

firstrecords := doReadPlainWord1(global(executionPlan[1],few));
nextRecords := doJoinReadPlainWord(global(executionPlan[2],few), firstRecords);

output(nextRecords);
