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

RETURN MODULE

import lib_stringlib;

export MaxTerms         := 50;
export MaxProximity     := 10;
export MaxWildcard      := 1000;
export MaxMatchPerDocument := 1000;
export MaxFilenameLength := 255;
export MaxActions        := 255;

export sourceType       := unsigned2;
export wordCountType    := unsigned8;
export segmentType      := unsigned1;
export wordPosType      := unsigned8;
export docPosType       := unsigned8;
export documentId       := unsigned8;
export termType         := unsigned1;
export distanceType     := integer8;
export indexWipType     := unsigned1;
export wipType          := unsigned8;
export stageType        := unsigned1;
export dateType         := unsigned8;
export charPosType      := unsigned1;           // position within a word

export sourceType docid2source(documentId x) := (x >> 48);
export unsigned8 docid2doc(documentId x) := (x & 0xFFFFFFFFFFFF);
export documentId createDocId(sourceType source, unsigned6 doc) := (documentId)(((unsigned8)source << 48) | doc);
export boolean docMatchesSource(unsigned8 docid, sourceType source) := (docid between createDocId(source,0) and (documentId)(createDocId(source+1,0)-1));

export wordType     := string20;
export wordFlags    := enum(unsigned1, HasLower=1, HasUpper=2);
export orderType    := enum(unsigned1, None=0, Precedes=1, Follows=2);

export savedWordPosType:= record
wordPosType          wpos;
docPosType           dpos;
//what else do we need to save?  segment?
                   end;
export wordIdType       := unsigned4;
export corpusFlags      := enum(unsigned1, isStopWord = 1);

export NameCorpusIndex      := '~GHcorpusIndex';
export NameWordIndex        := '~GHwordIndex';
export NameSentenceIndex    := '~GHsentenceIndex';
export NameParagraphIndex   := '~GHparagraphIndex';
export NameDocMetaIndex := '~GHmetaIndex';
export NameDateDocIndex := '~GHdateIndex';
export NameDocPosIndex      := '~GHdocPosIndex';
export NameTokenisedDocIndex    := '~GHtokenisedDocIndex';
export NameTokenIndex           := '~GHtokenIndex';


export corpusIndex      := index({ wordType word, wordIdType wordid, wordCountType numOccurences, wordCountType numDocuments, wordType normalized, corpusFlags flags } , NameCorpusIndex);
export wordIndex        := index({ wordType word, documentId doc, segmentType segment, wordPosType wpos } , { indexWipType wip, wordFlags flags, wordType original, docPosType dpos}, NameWordIndex);
export sentenceIndex    := index({ documentId doc, wordPosType wpos }, NameSentenceIndex);
export paragraphIndex   := index({ documentId doc, wordPosType wpos }, NameParagraphIndex);
export docMetaIndex := index({ documentId doc }, { string filename{maxlength(MaxFilenameLength)}, wordCountType numWords, dateType date, unsigned8 size }, NameDocMetaIndex);
export dateDocIndex := index({ dateType date }, { documentId doc }, NameDateDocIndex);

//May want the following, probably not actually implemented as an index - would save having dpos in the index, but more importantly storing it in the candidate match results because the mapping could be looked up later.
export docPosIndex      := index({ documentId doc, wordPosType wpos }, { docPosType dpos }, NameDocPosIndex);

//Again not implemented as an index (probably FETCHes from a flat file), but logically:
//tokenisedDocIndex := index({ documentId doc, wordPosType wpos}, { wordIdType wordid }, NameTokenisedDocIndex);

//may also need - probably implemented as a FETCH on a file, possibly on a memory based roxie file.
//tokenIndex        := index({ wordIdType wordid }, { wordType word, wordIdType normalized }, NameTokenIndex);

export wordIndexRecord := recordof(wordIndex);

///////////////////////////////////////////////////////////////////////////////////////////////////////////

export actionEnum := ENUM(
    None = 0,
//Minimal operations required to implement the searching.
    ReadWord,           // termNum, source, segment, word, wordFlagMask, wordFlagCompare,
    ReadQuotedWord,     // termNum, source, segment, word, original
    ReadWildWord,       // termNum, source, segment, word, wildMask, wildMatch, wordFlagMask, wordFlagCompare
    ReadDate,           // termNum, source, dateLow, dateHigh
    TermAndTerm,        // leftStage, rightStage,
    TermAndNotTerm,     // leftStage, rightStage
    TermAdjacentTerm,   // leftStage, leftTerm, rightStage, rightTerm, precedes
    TermOrTerm,         // leftStage, rightStage
    ProximityFilter,    // leftStage, leftTermSet, rightTermSet, precedes, distance
    SentanceFilter,     // leftStage, leftTerm, rightTerm               // more: terms could be sets in general case
    ParagraphFilter,    // leftStage, leftTerm, rightTerm
    Atleast,            // leftStage, minCount

//Fairly essential optimizations.
    TermAndProxTerm,    // leftStage, leftTerm, rightStage, rightTerm, precedes, distance
    TermAndNotProxTerm, // leftStage, leftTerm, rightStage, rightTerm, precedes, distance
    AtleastTerms,       // leftStage, leftTermSet, minCount                 // leftTermSet is set of terms the atleast is performed on

//Following are optimizations of the above
    TermAndWord,        // termNum, leftStage, precedes, segment, word, wordFlagMask, wordFlagCompare
    TermAndQuoted,      // termNum, leftStage, precedes, segment, word, original
    TermAndWildWord,    // termNum, leftStage, precedes, segment, word, wildMask, wildMatch, wordFlagMask, wordFlagCompare
    TermAdjacentWord,   // termNum, leftStage, leftTerm, source, segment, word, wordFlagMask, wordFlagCompare
    TermAdjacentQuoted, // termNum, leftStage, leftTerm, source, segment, word, original
    TermAndDate,        // leftStage, leftTerm, dateLow, dateHigh
    PassThrough,
    Last,

    //The following are only used in the production
    FlagModifier,       // wordFlagMask, wordFlagCompare
    QuoteModifier,      //
    Max
);

export integer numInputStages(actionEnum action) := CASE(action,
    actionEnum.ReadWord=>0,
    actionEnum.ReadQuotedWord=>0,
    actionEnum.ReadWildWord=>0,
    actionEnum.ReadDate=>0,
    actionEnum.TermAndTerm=>2,
    actionEnum.TermAdjacentTerm=>2,
    actionEnum.TermAndNotTerm=>2,
    actionEnum.TermAndProxTerm=>2,
    actionEnum.TermAndNotProxTerm=>2,
    actionEnum.TermOrTerm=>2,
    actionEnum.TermAndWord=>1,
    actionEnum.TermAndQuoted=>1,
    actionEnum.TermAndWildWord=>1,
    actionEnum.TermAdjacentWord=>1,
    actionEnum.TermAdjacentQuoted=>1,
    actionEnum.ProximityFilter=>1,
    actionEnum.SentanceFilter=>1,
    actionEnum.ParagraphFilter=>1,
    actionEnum.TermAndDate=>1,
    actionEnum.AtleastTerms=>1,
    actionEnum.Atleast=>1,
    actionEnum.PassThrough=>1,
    actionEnum.FlagModifier=>1,
    actionEnum.QuoteModifier=>1,
    0);
//  FAIL(stageType, 'Missing entry: ' + (string)action));

export boolean definesTerm(actionEnum action) :=
    (action in [actionEnum.ReadWord, actionEnum.ReadQuotedWord, actionEnum.ReadWildWord, actionEnum.ReadDate]);

export boolean isProximityOperator(actionEnum action) :=
    (action in [actionEnum.TermAndProxTerm, actionEnum.TermAndNotProxTerm, actionEnum.ProximityFilter]);

export actionEnum getQuotedAction(actionEnum action) :=
    CASE(action, actionEnum.ReadWord=>actionEnum.ReadQuotedWord,
                 actionEnum.TermAndWord=>actionEnum.TermAndQuoted,
                 actionEnum.TermAdjacentWord=>actionEnum.TermAdjacentQuoted,
                 action);

export invertPrecedes(orderType order) := IF(order <> orderType.None, 3-order, 0);

export termRecord := { termType term };

export searchRecord :=
            RECORD
stageType       stage;
actionEnum      action;
termType        term;

//The item being searched for
wordType        word;
wordType        original;
wordType        wildMask;
wordType        wildMatch;
charPosType     truncpos;
wordFlags       wordFlagMask;
wordFlags       wordFlagCompare;
sourceType      source;
segmentType     segment;

//Modifiers for date/value searching
dateType        dateLow;
dateType        dateHigh;

//Modifiers for atleast
wordCountType   minCount;

//Which results is this action applied to?
stageType       leftStage;
stageType       rightStage;
termType        leftTerm;
termType        rightTerm;
dataset(termRecord) leftTermSet{maxcount(MaxTerms)};
dataset(termRecord) rightTermSet{maxcount(MaxTerms)};

//Modifiers for the connector/filter
orderType       precedes;
distanceType    distance;

//Fields used by the optimizer
wordCountType   countEstimate;
boolean         changed;
            END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Matches

export docRecord := RECORD
documentId          doc;
                END;

export docPosRecord := RECORD
documentId          doc;
wordPosType         wpos;
                END;

export wordPosRecord    := RECORD
wordPosType         wpos;
                END;

export docPosSetRecord := RECORD
documentId          doc;
dataset(wordPosRecord)  positions{maxcount(MaxMatchPerDocument)};
                END;

export candidateBaseRecord :=
        record
documentId      doc;
stageType       stage;          // which stage last touched this row
        end;


export simpleCandidateRecord := candidateBaseRecord;

//Denormalized form.

export denormTermRecord :=
        record
termType        term;
unsigned4       cnt;            // used if doing atleast()
dataset(wordPosRecord) words{maxcount(MaxWildcard)};
        end;

//Normalized form....

export normTermRecord :=
        record
termType        term;
wordPosType     wpos;
        end;

export normCandidateRecord :=
        record(candidateBaseRecord)
dataset(normTermRecord) matches{maxcount(MaxTerms)};
        end;


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Functions which are helpful for hand constructing queries...

export CmdReadWord(termType term, sourceType source, segmentType segment, wordType word, wordFlags wordFlagMask = 0, wordFlags wordFlagCompare = 0) :=
    TRANSFORM(searchRecord,
                SELF.action := actionEnum.ReadWord;
                SELF.term := term;
                SELF.source := source;
                SELF.segment := segment;
                SELF.word := word;
                SELF.wordFlagMask := wordFlagMask;
                SELF.wordFlagCompare:= wordFlagCompare;
                SELF := []);

export CmdReadQuoted(termType term, sourceType source, segmentType segment, wordType word, wordType original) :=
    TRANSFORM(searchRecord,
                SELF.action := actionEnum.ReadWord;
                SELF.term := term;
                SELF.source := source;
                SELF.segment := segment;
                SELF.word := word;
                SELF.original := original;
                SELF.wordFlagMask := 0;
                SELF.wordFlagCompare:= 0;
                SELF := []);

export CmdReadWildWord(termType term, sourceType source, segmentType segment, wordType word, wordType wildMask, wordType wildMatch, wordFlags wordFlagMask = 0, wordFlags wordFlagCompare = 0) :=
    TRANSFORM(searchRecord,
                SELF.action := actionEnum.ReadWord;
                SELF.term := term;
                SELF.source := source;
                SELF.segment := segment;
                SELF.word := word;
                SELF.wildMask:= wildMask;
                SELF.wildMatch:= wildMatch;
                SELF.wordFlagMask := wordFlagMask;
                SELF.wordFlagCompare:= wordFlagCompare;
                SELF := []);

export CmdReadDate(termType term, sourceType source, dateType dateLow, dateType dateHigh) :=
    TRANSFORM(searchRecord,
                SELF.action := actionEnum.ReadWord;
                SELF.term := term;
                SELF.source := source;
                SELF.dateLow := dateLow;
                SELF.dateHigh := dateHigh;
                SELF := []);

shared defineCmdTermCombineTerm(actionEnum action, termType term, stageType leftStage, termType leftTerm, stageType rightStage, termType rightTerm, orderType precedes = orderType.None, distanceType distance = 0) :=
    TRANSFORM(searchRecord,
                SELF.action := action;
                SELF.term := term;
                SELF.leftStage := leftStage;
                SELF.leftTerm := leftTerm;
                SELF.rightStage := rightStage;
                SELF.rightTerm := rightTerm;
                SELF.precedes := precedes;
                SELF.distance := distance;
                SELF := []);

export CmdTermAndTerm(stageType leftStage, stageType rightStage) :=
    defineCmdTermCombineTerm(actionEnum.TermAndTerm, 0, leftStage, 0, rightStage, 0);

export CmdTermAndNotTerm(stageType leftStage, stageType rightStage) :=
    defineCmdTermCombineTerm(actionEnum.TermAndNotTerm, 0, leftStage, 0, rightStage, 0);

export CmdTermAdjacentTerm(stageType leftStage, termType leftTerm, stageType rightStage, termType rightTerm, orderType precedes) :=
    defineCmdTermCombineTerm(actionEnum.TermAdjacentTerm, 0, leftStage, leftTerm, rightStage, rightTerm, precedes);

export CmdTermAndProxTerm(stageType leftStage, termType leftTerm, stageType rightStage, termType rightTerm, orderType precedes, distanceType distance) :=
    defineCmdTermCombineTerm(actionEnum.TermAndProxTerm, 0, leftStage, leftTerm, rightStage, rightTerm, precedes, distance);

export CmdTermAndNotProxTerm(stageType leftStage, termType leftTerm, stageType rightStage, termType rightTerm, orderType precedes, distanceType distance) :=
    defineCmdTermCombineTerm(actionEnum.TermAndNotProxTerm, 0, leftStage, leftTerm, rightStage, rightTerm, precedes, distance);

export CmdTermOrTerm(stageType leftStage, stageType rightStage) :=
    defineCmdTermCombineTerm(actionEnum.TermOrTerm, 0, leftStage, 0, rightStage, 0);

export CmdSentanceFilter(stageType leftStage, termType leftTerm, termType rightTerm, orderType precedes = orderType.None) :=
    defineCmdTermCombineTerm(actionEnum.SentanceFilter, 0, leftStage, leftTerm, 0, rightTerm, precedes);

export CmdParagraphFilter(stageType leftStage, termType leftTerm, termType rightTerm, orderType precedes = orderType.None) :=
    defineCmdTermCombineTerm(actionEnum.ParagraphFilter, 0, leftStage, leftTerm, 0, rightTerm, precedes);

export CmdProximityFilter(stageType leftStage, set of termType leftTermSet, set of termType rightTermSet, orderType precedes, unsigned distance) :=
    TRANSFORM(searchRecord,
                SELF.action := actionEnum.ProximityFilter;
                SELF.leftStage := leftStage;
                SELF.leftTermSet := dataset(leftTermSet, termRecord);
                SELF.rightTermSet := dataset(rightTermSet, termRecord);
                SELF.precedes := precedes;
                SELF.distance := distance;
                SELF := []);

shared defineCmdTermAndWord(actionEnum action, termType term, stageType leftStage, termType leftTerm, orderType precedes = orderType.None, distanceType distance = 0, segmentType segment, wordType word, wordFlags wordFlagMask = 0, wordFlags wordFlagCompare = 0, wordType original = '') :=
    TRANSFORM(searchRecord,
                SELF.action := action;
                SELF.term := term;
                SELF.segment := segment;
                SELF.precedes := precedes;
                SELF.distance := distance;
                SELF.word := word;
                SELF.wordFlagMask := wordFlagMask;
                SELF.wordFlagCompare:= wordFlagCompare;
                SELF.leftStage := leftStage;
                SELF.leftTerm := leftTerm;
                SELF.original := original;
                SELF := []);

export CmdTermAndWord(termType term, stageType leftStage, segmentType segment, wordType word, wordFlags wordFlagMask = 0, wordFlags wordFlagCompare = 0) :=
    defineCmdTermAndWord(actionEnum.TermAndWord, term, leftStage, 0, , , segment, word, wordFlagMask, wordFlagCompare);

export CmdTermAndQuoted(termType term, stageType leftStage, segmentType segment, wordType word, wordType original) :=
    defineCmdTermAndWord(actionEnum.TermAndWord, term, leftStage, 0, , , segment, word, , , original);

export CmdTermAdjacentWord(termType term, stageType leftStage, termType leftTerm, orderType precedes, segmentType segment, wordType word, wordFlags wordFlagMask = 0, wordFlags wordFlagCompare = 0) :=
    defineCmdTermAndWord(actionEnum.TermAdjacentWord, term, leftStage, leftTerm, precedes, , segment, word, wordFlagMask, wordFlagCompare);

export CmdTermAdjacentQuoted(termType term, stageType leftStage, termType leftTerm, orderType precedes, segmentType segment, wordType word, wordType original) :=
    defineCmdTermAndWord(actionEnum.TermAdjacentWord, term, leftStage, leftTerm, precedes, , segment, word, , , original);

export CmdAtleast(stageType leftStage, wordCountType minCount) :=
    TRANSFORM(searchRecord,
                SELF.action := actionEnum.Atleast;
                SELF.leftStage := leftStage;
                SELF.minCount := minCount;
                SELF := []);

export CmdAtleastTerm(stageType leftStage, set of termType terms, wordCountType minCount) :=
    TRANSFORM(searchRecord,
                SELF.action := actionEnum.AtleastTerms;
                SELF.leftStage := leftStage;
                SELF.leftTermSet := dataset(terms, termRecord);
                SELF.minCount := minCount;
                SELF := []);


//-------------------------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------------------------
//----------------------------------- Code for optimizing queries ---------------------------------------------
//-------------------------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------------------------


shared renumberEntries(dataset(searchRecord) query, integer delta) :=
    project(query, transform(searchRecord, self.stage := left.stage + delta; self.changed := true; self := left));

shared removeEntries(dataset(searchRecord) query, stageType newStage) :=
    project(query, transform(recordof(query), self.action := actionEnum.Passthrough, self.leftStage := newStage; self.changed := true; self := left));


shared removeNullEntries(dataset(searchRecord) query) := function
    countRecord := { unsigned2 cnt; };
    removeCount := project(query, transform(countRecord, self.cnt := if(left.action = actionEnum.Passthrough, 1, 0)));
    deltas := iterate(removeCount, transform(countRecord, self.cnt := left.cnt + right.cnt));

    searchRecord adjustStagesTransform(searchRecord l, integer _cnt) :=
        TRANSFORM,SKIP(l.action = actionEnum.PassThrough)
            cnt := l.stage;     // if we used the passed in cnt it doesn't work counter is not seed to be used...
            SELF.stage := l.stage - deltas[l.stage].cnt;
            SELF.leftStage := if(l.leftStage <> 0, l.leftStage - deltas[l.leftStage].cnt, l.leftStage);
            SELF.rightStage := if(l.rightStage <> 0, l.rightStage - deltas[l.rightStage].cnt, l.rightStage);
            SELF := l;
        END;
    return project(query, adjustStagesTransform(left, counter));
end;

invertProximity(dataset(searchRecord) query) :=
    project(query, transform(searchRecord, self.precedes := invertPrecedes(left.precedes), self := left));

swapEntries(dataset(searchRecord) query, integer x, integer y, integer z) := function
    low := if (x < y, x, y);
    high := if (x < y, y, x);

    return  query[1..low-1] +
            renumberEntries(query[high..high], low-high) +
            query[low+1..high-1] +
            renumberEntries(query[low..low], high-low) +
            query[high+1..z-1] +
            invertProximity(query[z..z]) +
            query[z+1..];
end;

optimizeSwapInputs(dataset(searchRecord) query, searchRecord cur) := function
    lhs := query[cur.leftStage];
    rhs := query[cur.rightStage];

    worthSwapping :=
        (cur.action in [actionEnum.TermAndTerm,actionEnum.TermAdjacentTerm,actionEnum.TermOrTerm,actionEnum.TermAndProxTerm]) and
        ((lhs.action = actionEnum.ReadWord and
          rhs.action = actionEnum.ReadWord and
          lhs.countEstimate > rhs.countEstimate) or
          lhs.action = actionEnum.ReadWord and
          rhs.action <> actionEnum.ReadWord);

    return if (worthSwapping, swapEntries(query, cur.leftStage, cur.rightStage, cur.stage), query);
end;

optimizeTermAndWord(dataset(searchRecord) query, searchRecord cur) := function
    lhs := query[cur.leftStage];
    rhs := query[cur.rightStage];

    optimizedAction := case(cur.action,
        actionEnum.TermAndTerm=>
            case (rhs.action,
                actionEnum.ReadWord => actionEnum.TermAndWord,
                actionEnum.None),
        actionEnum.None);

    createCombinedAction() := row(transform(searchRecord,
            self.stage := cur.stage;
            self.leftStage := cur.leftStage;
            self.action := optimizedAction;
            self := rhs));

    return if (optimizedAction != actionEnum.None,
        query[1..cur.rightStage-1] + removeEntries(query[cur.rightStage..cur.rightStage], 0) +
        query[cur.rightStage+1..cur.stage-1] + createCombinedAction() +
        query[cur.stage+1..],
        query);
end;

optimizeEntry(dataset(searchRecord) query, searchRecord cur) :=
    optimizeTermAndWord(optimizeSwapInputs(query, cur), cur);

optimizeSequence(dataset(searchRecord) query) := function
    unchanged := project(query, transform(searchRecord, self.changed := false; self := left));
    ret := LOOP(unchanged, count(unchanged), optimizeEntry(rows(left), rows(left)[noboundcheck counter]));
    return removeNullEntries(ret);
end;


export optimizeQuery(dataset(searchRecord) query) := function
    return LOOP(query, true, counter = 1 or exists(rows(left)(changed)), optimizeSequence(rows(left)));
end;

export cleanupUnusedEntries(dataset(searchRecord) query) :=
    removeNullEntries(query);

export annotateQuery(dataset(searchRecord) query) := function
    searchRecord createAnnotated(searchRecord l, corpusIndex r) := transform
            self.countEstimate := r.numDocuments;
            self := l;
        end;

    return join(query, corpusIndex, keyed(left.word <> '' and left.word = right.word), createAnnotated(left, right), left outer);
end;

//-------------------------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------------------------
//----------------------------------- Code for parsing queries ---------------------------------------------
//-------------------------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------------------------

export parseLNQuery(string queryText) := function

productionRecord  :=
            record
dataset(searchRecord) actions{maxcount(MaxActions)};
            end;

unknownTerm := (termType)-1;

PRULE := rule type (productionRecord);
ARULE := rule type (searchRecord);

CmdDummy() := transform(searchRecord, self := []);
CmdSimple(actionEnum action) := transform(searchRecord, self.action := action; self := []);

///////////////////////////////////////////////////////////////////////////////////////////////////////////

pattern ws := [' ','\t'];

token number    := pattern('[0-9]+');
token wordpat   := pattern('[A-Za-z]+');
//token date := pattern('[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}');
token date      := pattern('[0-9][0-9]?/[0-9][0-9]?/[0-9][0-9]([0-9][0-9])?');
token wildcarded := pattern('[A-Z]+[*][*a-z]*');
token suffixed  := pattern('[A-Za-z]+!');

token atleast   := 'ATLEAST' pattern('[0-9]+');
token capsBra   := 'CAPS(';
token noCapsBra := 'NOCAPS(';

token quoteChar := '"';

ProdProximityFilter(orderType precedes, unsigned distance) := CmdProximityFilter(0, [], [], precedes, distance);
ProdTermAdjacentTerm(orderType precedes) := CmdTermAdjacentTerm(0, 0, 0, 0, precedes);

///////////////////////////////////////////////////////////////////////////////////////////////////////////

rule compare
    :=  '='
    |   '<>'
    |   '<'
    |   '>'
    |   '>='
    |   '<='
    |   'is'
    |   'aft'
    |   'bef'
    ;

ARULE proximity
    := 'PRE/'                                   ProdProximityFilter(orderType.Precedes, 0)
    |  pattern('PRE/[0-9]+')                    ProdProximityFilter(orderType.Precedes, (integer)$1[5..])
    |  'PRE/P'                                  CmdParagraphFilter(0, 0, 0, orderType.Precedes)
    |  'PRE/S'                                  CmdSentanceFilter(0, 0, 0, orderType.Precedes)
    |  pattern('/[0-9]+')                       ProdProximityFilter(orderType.None, (integer)$1[2..])
    |  '/P'                                     CmdParagraphFilter(0, 0, 0, orderType.None)
    |  '/S'                                     CmdSentanceFilter(0, 0, 0, orderType.None)
    ;

rule numericValue
    := number
    |  '$' number
    |  number 'mm'
    ;

rule numericRange
    := numericValue '..' numericValue
    ;

PRULE forwardExpr := use(productionRecord, 'ExpressionRule');

rule segmentName
//  := wordpat not in ['CAPS','NOCAPS','ALLCAPS']
    := validate(wordpat, StringLib.StringToLowerCase(matchtext) not in ['caps','nocaps','allcaps']);
    ;

//MORE: Need to implement this for efficiency - both case and otherwise.  Should be ok as long as restrictive set used.
//rule word := wordpat not in ['AND', 'OR', 'NOT'];
rule word := validate(wordpat, StringLib.StringToLowerCase(matchtext) not in ['and', 'or', 'not']);

ARULE term0
    := word                                     CmdReadWord(unknownTerm, 0, 0, $1, 0, 0)
    |  suffixed                                 CmdReadWildWord(unknownTerm, 0, 0, $1[1..length(trim($1))-1], '', '')
    |  wildcarded                               CmdSimple(__LINE__*100)
    |  numericRange                             CmdSimple(__LINE__*100)
    ;

PRULE term1
    := term0                                    transform(productionRecord, self.actions := dataset($1))
    | '(' forwardExpr ')'
    | 'CAPS' '(' forwardExpr ')'                transform(productionRecord, self.actions := $3.actions + row(
                                                    transform(searchRecord,
                                                        self.action := actionEnum.FlagModifier;
                                                        self.wordFlagMask := wordFlags.HasUpper,
                                                        self.wordFlagCompare := wordFlags.HasUpper,
                                                        self := []
                                                    )
                                                ))
    | 'NOCAPS' '(' forwardExpr ')'              transform(productionRecord, self.actions := $3.actions + row(
                                                    transform(searchRecord,
                                                        self.action := actionEnum.FlagModifier;
                                                        self.wordFlagMask := wordFlags.HasUpper,
                                                        self.wordFlagCompare := 0,
                                                        self := []
                                                    )
                                                ))
    | 'ALLCAPS' '(' forwardExpr ')'             transform(productionRecord, self.actions := $3.actions + row(
                                                    transform(searchRecord,
                                                        self.action := actionEnum.FlagModifier;
                                                        self.wordFlagMask := wordFlags.HasLower+wordFlags.HasUpper,
                                                        self.wordFlagCompare := wordFlags.HasUpper,
                                                        self := []
                                                    )
                                                ))
    | segmentName '(' forwardExpr ')'           transform(productionRecord, self.actions := $3.actions + row(
                                                    CmdSimple(__LINE__*100)
                                                ))
    | atleast '(' forwardExpr ')'               transform(productionRecord, self.actions := $3.actions + row(
                                                    CmdAtLeast(0, (integer)$1[8..])
                                                ))
    ;


PRULE phrase
    :=  term1
    |   SELF term1                              transform(productionRecord, self.actions := $1.actions + $2.actions + row(ProdTermAdjacentTerm(orderType.Precedes)))
    ;

PRULE qphrase
    := term0                                    transform(productionRecord, self.actions := dataset($1))
    | SELF term0                                transform(productionRecord, self.actions := $1.actions + $2 + row(ProdTermAdjacentTerm(orderType.Precedes)))
    //MORE: We need to skip () etc. and also allow the words and/or
    ;

rule condition
    := word compare number
    |  word compare date
    ;

PRULE term
    :=  phrase
    |   quoteChar qphrase quoteChar             transform(productionRecord, self.actions := $2.actions + row(CmdSimple(actionEnum.QuoteModifier)))
//  |   condition
    ;

PRULE combined
    := term
    |  SELF 'AND' term                          transform(productionRecord, self.actions := $1.actions + $3.actions + row(CmdSimple(actionEnum.TermAndTerm)))
    |  SELF proximity term                      transform(productionRecord,
                                                        self.actions := $1.actions + $3.actions + row(CmdSimple(actionEnum.TermAndTerm)) + $2
                                                )
    |  SELF 'AND' 'NOT' term                    transform(productionRecord, self.actions := $1.actions + $4.actions + row(CmdSimple(actionEnum.TermAndNotTerm)))
    |  SELF 'AND' 'NOT' proximity term          transform(productionRecord, self.actions := $1.actions + $5.actions + row(CmdSimple(actionEnum.TermAndNotProxTerm)))
    |  SELF 'OR' term                           transform(productionRecord, self.actions := $1.actions + $3.actions + row(CmdSimple(actionEnum.TermOrTerm)))
    ;

PRULE expr
    := combined                                 : define ('ExpressionRule')
    ;

infile := dataset(row(transform({ string line{maxlength(1023)} }, self.line := queryText)));

resultsRecord := record
dataset(searchRecord) actions{maxcount(MaxActions)};
        end;


resultsRecord extractResults(dataset(searchRecord) actions) :=
        TRANSFORM
            SELF.actions := actions;
        END;

p1 := PARSE(infile,line,expr,extractResults($1.actions),first,whole,skip(ws),nocase,parse);

pnorm := normalize(p1, left.actions, transform(right));

//Allocate sequence numbers and term ids to the terms.
stageRecord := { stageType stage };
termsRecord := { dataset(termRecord) terms{maxcount(MaxTerms)}; };
extraRecord := record(searchRecord)
dataset(stageRecord) stages{maxcount(MaxActions)};
dataset(termsRecord) leftTermsUsed{maxcount(MaxTerms)};
dataset(termsRecord) rightTermsUsed{maxcount(MaxTerms)};
termType termCount;
        end;

pextra := project(pnorm, transform(extraRecord, self := left, self := []));

//Note:
//This would need extending if the parser ever generates anything behond the core operations
//Especially the code for keeping track of the terms.
extraRecord addSequence(extraRecord l, extraRecord r) := transform
        numInputs := numInputStages(r.action);
        thisStage := l.stage + 1;
        leftStageCount := count(l.stages);
        thisTerm := IF(definesTerm(r.action), l.termCount+1, 0);
        thisTerms := IF(thisTerm <> 0, dataset(row(transform(termRecord, self.term := thisTerm))));
        leftStage := CASE(numInputs, 1=>l.stages[leftStageCount].stage, 2=>l.stages[leftStageCount-1].stage, 0);
        rightStage := CASE(numInputs, 2=>l.stages[leftStageCount].stage, 0);
        SELF.stage := thisStage;
        SELF.stages := l.stages[1..leftStageCount-numInputs] + row(transform(stageRecord, self.stage := thisStage));
        SELF.term := thisTerm;
        SELF.termCount := IF(thisTerm <> 0, thisTerm, l.termCount);
        SELF.leftStage := leftStage;
        SELF.rightStage := rightStage;

        newLeftTerms := MAP(
                    numInputs = 2=>l.leftTermsUsed[leftStage].terms + l.rightTermsUsed[leftStage].terms,
                    numInputs = 1=>l.leftTermsUsed[leftStage].terms,
                    thisTerms);
        SELF.leftTermsUsed := l.leftTermsUsed + row(transform(termsRecord, self.terms := newLeftTerms));
        newRightTerms := MAP(
                    numInputs = 2=>l.leftTermsUsed[rightStage].terms + l.rightTermsUsed[rightStage].terms,
                    numInputs = 1=>l.rightTermsUsed[leftStage].terms + thisTerms);
        SELF.rightTermsUsed := l.rightTermsUsed + row(transform(termsRecord, self.terms := newRightTerms));
        SELF.leftTermSet := IF(isProximityOperator(r.action), newLeftTerms);
        SELF.rightTermSet := IF(isProximityOperator(r.action), newRightTerms);
        SELF := r;
    END;

pseq := iterate(pextra, addSequence(LEFT, RIGHT));


//Percolate caps, quote etc. down the tree and normalize the search words where appropriate
//First create a list of terms that need the caps/quote actions applied to them.
//Then order and roll them up so that closer operations take precedence.
//finally join the sequence to the actions

ac := pseq(action in [actionEnum.FlagModifier, actionEnum.QuoteModifier]);

acp := project(ac, transform(extraRecord, SELF.leftTermSet := left.leftTermsUsed[left.stage].terms + left.rightTermsUsed[left.stage].terms; self := left));

actRecord := record
termType        term;
stageType       stage;
wordFlags       wordFlagMask;
wordFlags       wordFlagCompare;
boolean         isQuote;
            end;


actRecord createModifiers(extraRecord l, termRecord r) := transform
    SELF.term := r.term;
    SELF.isQuote := l.action = actionEnum.QuoteModifier;
    SELF.stage := l.stage;
    SELF.wordFlagMask := l.wordFlagMask;
    SELF.wordFlagCompare := l.wordFlagMask;
    end;

norm := normalize(acp, left.leftTermSet, createModifiers(LEFT, RIGHT));

smod := sort(norm, term, stage);

gmod := group(smod, term);

rmod := rollup(gmod, true, transform(actRecord,
                        self.isQuote := left.isQuote or right.isQuote,
                        self.wordFlagMask := if (left.wordFlagMask <> 0, left.wordFlagMask, right.wordFlagMask),
                        self.wordFlagCompare := if (left.wordFlagMask <> 0, left.wordFlagCompare, right.wordFlagCompare),
                        self := left));


extraRecord applyModifiers(extraRecord l, actRecord r) := TRANSFORM
        SELF.word := IF(r.isQuote, l.word, StringLib.StringToLowerCase(l.word));
        SELF.wordFlagMask := r.wordFlagMask;
        SELF.wordFlagCompare := r.wordFlagCompare;
        SELF.action :=  IF(l.action in [actionEnum.FlagModifier, actionEnum.QuoteModifier],
                            actionEnum.PassThrough,
                            IF(r.isQuote,
                                getQuotedAction(l.action),
                                l.action));
        SELF := l;
    END;

annotated := JOIN(pseq, rmod, left.term = right.term, applyModifiers(left, right), left outer, lookup);

//Remove all the housekeeping.
searchRecord removeExtra(extraRecord l) := transform
        SELF := l;
    END;

pout := project(annotated, removeExtra(LEFT));

//final := cleanupUnusedEntries(pout);
final := cleanupUnusedEntries(global(pout, few));

return final;
end;

//-------------------------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------------------------
//---------------------------------------- Code for executing queries -----------------------------------------
//-------------------------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------------------------

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Matching helper functions

shared matchSingleWordFlags(wordIndex wIndex, searchRecord search) :=
    keyed(search.segment = 0 or wIndex.segment = search.segment, opt) AND
    ((wIndex.flags & search.wordFlagMask) = search.wordFlagCompare);

shared matchSingleWord(wordIndex wIndex, searchRecord search) :=
    keyed(wIndex.word = search.word) AND
    matchSingleWordFlags(wIndex, search);

shared matchFirstWord(wordIndex wIndex, searchRecord search) :=
    keyed(search.source = 0 OR docMatchesSource(wIndex.doc, search.source), opt);

shared matchDateRange(dateType date, searchRecord search) :=
    ((date >= search.dateLow) and (date <= if(search.dateHigh = 0, (dateType)-1, search.dateHigh)));

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Some common functions

shared minimalMatchRecord :=
        record
documentId      doc;
wordPosType     wPos;
        end;


shared denormTermRecord wordsToDenormTerm(dataset(minimalMatchRecord) in, termType term) :=
        TRANSFORM
            SELF.term := term;
            SELF.cnt := count(in);
            SELF.words := project(in, transform(wordPosRecord, self := left));
        END;

shared normTermRecord wordToNormTerm(wordIndexRecord match, termType term) :=
        TRANSFORM
            SELF.term := term;
            SELF.wpos := match.wpos;
        END;

shared normCandidateRecord wordToNormCandidate(wordIndexRecord match, termType term, stageType stage) :=
        TRANSFORM
            SELF.stage := stage;
            SELF.doc := match.doc;
            SELF.matches := dataset(row(wordToNormTerm(match, term)));
        END;

shared createNormCandidateFromWordMatches(searchRecord search, dataset(wordIndexRecord) matches) := function

    return project(matches, wordToNormCandidate(LEFT, search.term, search.stage));

END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

shared doNormReadWord(searchRecord search) := FUNCTION

    matches := sorted(wordIndex)(
                        matchSingleWord(wordIndex, search) AND
                        matchFirstWord(wordIndex, search));

    //Because word is single valued, then must be sorted by document
    sortedMatches := sorted(matches, doc);

    return createNormCandidateFromWordMatches(search, sortedMatches);
END;


///////////////////////////////////////////////////////////////////////////////////////////////////////////

shared doNormReadQuotedWord(searchRecord search) := FUNCTION
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
shared boolean doMatchWildcards(unsigned4 len, wordType word, wordType wildMask, wordType wildMatch) := BEGINC++

    for (unsigned i=0; i < len; i++)
    {
        if ((word[i] & wildmask[i]) != wildmatch[i])
            return false;
    }
    return true;

ENDC++;

shared boolean matchWildcards(wordType word, wordType wildMask, wordType wildMatch) :=
    doMatchWildcards(length(wildMask), word, wildMask, wildMatch);


shared doNormReadWildWord(searchRecord search) := FUNCTION
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

shared doNormReadDate(searchRecord search) := function
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

shared boolean validAdjacent(searchRecord search, wordPosType lwPos, wordPosType rwPos) := function
    return rwPos = IF(search.precedes=orderType.Precedes, lwPos+1, lwPos -1);
END;

shared boolean validProximity(searchRecord search, wordPosType lwPos, wordPosType rwPos) := function

    wordPosType lowerBound(wordPosType oldpos) :=
        IF (search.precedes = orderType.Precedes, oldpos+1, IF(oldpos >= search.distance, oldpos-search.distance, 1));

    wordPosType upperBound(wordPosType oldpos) :=
        IF (search.precedes = orderType.Follows, oldpos-1, oldpos+search.distance);

    return
        ((search.distance = 0 and (search.precedes != orderType.Precedes)) OR
                rwPos >= lowerBound(lwPos)) AND
        ((search.distance = 0 and (search.precedes != orderType.Follows)) OR
                rwPos <= upperBound(lwpos));
END;

shared boolean validDenormAdjacent(searchRecord search, denormTermRecord l, wordPosType rwPos) := function
    return exists(l.words(validAdjacent(search, wpos, rwPos)));
END;

shared boolean validDenormProximity(searchRecord search, denormTermRecord l, wordPosType rwPos) := function
    return exists(l.words(validProximity(search, wpos, rwPos)));
END;

shared boolean validNormAdjacent(searchRecord search, normTermRecord l, wordPosType rwPos) := function
    return validAdjacent(search, l.wpos, rwPos);
END;

shared boolean validNormProximity(searchRecord search, normTermRecord l, wordPosType rwPos) := function
    return validProximity(search, l.wpos, rwPos);
END;

shared normCandidateRecord combineNormCandidateTransform(stageType stage, normCandidateRecord l, normCandidateRecord r) := TRANSFORM
        SELF.doc := IF(l.doc <> 0, l.doc, r.doc);
        SELF.stage := stage;
        SELF.matches := l.matches + r.matches;
    END;

shared normCandidateRecord extendNormCandidateTransform(stageType stage, normCandidateRecord l, termType term, wordPosType wpos) := TRANSFORM
        SELF.doc := l.doc;
        SELF.stage := stage;
        SELF.matches := l.matches + row(transform(normTermRecord, self.term := term, self.wpos := wpos));
    END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

shared doNormTermAndTerm(searchRecord search, dataset(normCandidateRecord) in) := function
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    inRight := sort(in(stage = search.rightStage), doc);        // NB: change to sorted() not sort
    others := in(stage not in [search.leftStage, search.rightStage]);       // generates poor code: why?

    matches := join(inLeft, inRight, left.doc = right.doc, combineNormCandidateTransform(search.stage, LEFT, RIGHT), INNER);
    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

shared doNormTermAndNotTerm(searchRecord search, dataset(normCandidateRecord) in) := function
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    inRight := sort(in(stage = search.rightStage), doc);        // NB: change to sorted() not sort
    others := in(stage not in [search.leftStage, search.rightStage]);       // generates poor code: why?

    matches := join(inLeft, inRight, left.doc = right.doc, combineNormCandidateTransform(search.stage, LEFT, RIGHT), LEFT ONLY);
    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

shared doNormTermAndProxTerm(searchRecord search, dataset(normCandidateRecord) in) := function
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

shared doNormTermAndNotProxTerm(searchRecord search, dataset(normCandidateRecord) in) := function
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
shared doNormProximityFilter(searchRecord search, dataset(normCandidateRecord) in) := function
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    others := in(stage != search.leftStage);

    matchesProximityFilter(dataset(normTermRecord) l, dataset(normTermRecord) r) :=
        exists(choosen(join(l, r, validProximity(search, left.wpos, right.wpos), all), 1));

    filtered := inLeft(matchesProximityFilter(matches(term in set(search.leftTermSet, term)), matches(term in set(search.rightTermSet, term))));
    matches := project(filtered, transform(recordof(in), self.stage := search.stage, self := left));
    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

shared doNormTermOrTerm(searchRecord search, dataset(normCandidateRecord) in) := FUNCTION
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    inRight := sort(in(stage = search.rightStage), doc);        // NB: change to sorted() not sort
    others := in(stage not in [search.leftStage, search.rightStage]);       // generates poor code: why?

    matches := join(inLeft, inRight, left.doc = right.doc, combineNormCandidateTransform(search.stage, LEFT, RIGHT), FULL OUTER);
    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

shared doNormTermAndWordBad(searchRecord search, dataset(normCandidateRecord) in) := function
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    others := in(stage != search.leftStage);

    matches := join(inLeft, wordIndex,
                    keyed(left.doc = right.doc) AND
                    matchSingleWord(RIGHT, search),
                    extendNormCandidateTransform(search.stage, LEFT, search.term, right.wpos), INNER);
    return others + matches;
END;

shared doNormTermAndWord(searchRecord search, dataset(normCandidateRecord) in) := function
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

shared doNormTermAdjacentWord(searchRecord search, dataset(normCandidateRecord) in) := function
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

shared wposInRange(searchRecord search, normCandidateRecord l, wordPosType wpos) := function
    leftMatch := l.matches(term = search.leftTerm)[1];
    leftMatchPos := leftMatch.wpos;
    rightMatch := l.matches(term = search.rightTerm)[1];
    rightMatchPos := rightMatch.wpos;
    lowerPos := if (leftMatchPos < rightMatchPos, leftMatchPos, rightMatchPos);
    upperPos := if (leftMatchPos < rightMatchPos, rightMatchPos, leftMatchPos);
    return (wpos > lowerPos) and (wpos <= upperPos);
END;


shared doNormSentanceFilter(searchRecord search, dataset(normCandidateRecord) in) := FUNCTION
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    others := in(stage <> search.leftStage);

    matches := join(inLeft, sentenceIndex,
                    keyed(left.doc = right.doc) AND
                    (wposInRange(search, left, right.wpos)),            // make this keyed when wpos not in payload
                    transform(normCandidateRecord, self.stage := search.stage; self := left), LEFT ONLY);
    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

shared doNormParagraphFilter(searchRecord search, dataset(normCandidateRecord) in) := FUNCTION
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    others := in(stage <> search.leftStage);

    matches := join(inLeft, paragraphIndex,
                    keyed(left.doc = right.doc) AND
                    (wposInRange(search, left, right.wpos)),            // make this keyed when wpos not in payload
                    transform(normCandidateRecord, self.stage := search.stage; self := left), LEFT ONLY);
    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

shared doNormTermAndDate(searchRecord search, dataset(normCandidateRecord) in) :=
    FAIL(recordof(in), 'Action doTermAndDate not yet implemented');

///////////////////////////////////////////////////////////////////////////////////////////////////////////

shared doNormAtleast(searchRecord search, dataset(normCandidateRecord) in) := function
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

shared doNormAtleastTerms(searchRecord search, dataset(normCandidateRecord) in) := function
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    others := in(stage <> search.leftStage);

    //type transfer is horrible, but enables us to group by a dataset...
    gr1 := group(inLeft, doc);
    pr1 := project(gr1, transform(normCandidateRecord, self.matches := left.matches(term not in set(search.leftTermSet, term)); self := left));
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

shared processStageNorm(searchRecord search, dataset(normCandidateRecord) in) :=
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

export ExecuteReduceFullQuery(dataset(searchRecord) queryDefinition, dataset(normCandidateRecord) initialResults) := function

    executionPlan := global(queryDefinition, few);          // Store globally for efficient access

    results := LOOP(initialResults, count(executionPlan), processStageNorm(executionPlan[NOBOUNDCHECK COUNTER], rows(left)));


    userOutputMatchRecord   := RECORD
    termType                term;
    docPosType              dpos;
                        END;

    userOutputRecord :=
            record
    string  name{maxlength(MaxFilenameLength)};
    dataset(userOutputMatchRecord)  positions{maxcount(MaxMatchPerDocument)};           // which stage last touched this row
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



export ExecuteFullQuery(dataset(searchRecord) queryDefinition) := function

    initialResults := dataset([], normCandidateRecord);

    return ExecuteReduceFullQuery(queryDefinition, initialResults);

end;

///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////

shared simpleCandidateRecord createSimpleCandidateTransform(stageType stage, simpleCandidateRecord l) := TRANSFORM
        SELF.doc := l.doc;
        SELF.stage := stage;
    END;

shared simpleCandidateRecord combineSimpleCandidateTransform(stageType stage, simpleCandidateRecord l, simpleCandidateRecord r) := TRANSFORM
        SELF.doc := IF(l.doc <> 0, l.doc, r.doc);
        SELF.stage := stage;
    END;


shared reduceMultipleToSingle(searchRecord search, dataset(recordof(wordIndex)) matches) := FUNCTION
    reduced := project(matches, transform(docRecord, self := left));

    //Because word is single valued, then must be sorted by document
    sortedMatches := sorted(reduced, doc);
    groupedMatches := group(sortedMatches, doc);
    deduped := rollup(groupedMatches, group, transform(simpleCandidateRecord, self.doc := left.doc; self.stage := search.stage));
    return deduped;
end;

shared doSimpleReadWordX(searchRecord search, boolean checkOriginal) := FUNCTION

    matches := sorted(wordIndex)(
                        matchSingleWord(wordIndex, search) AND
                        matchFirstWord(wordIndex, search) and
                        (not checkOriginal or wordIndex.original = search.original));

    return reduceMultipleToSingle(search, matches);
END;


shared doSimpleReadWord(searchRecord search) :=
    doSimpleReadWordX(search, false);


///////////////////////////////////////////////////////////////////////////////////////////////////////////

shared doSimpleReadQuotedWord(searchRecord search) :=
    doSimpleReadWordX(search, true);


///////////////////////////////////////////////////////////////////////////////////////////////////////////

shared doSimpleReadWildWord(searchRecord search) := FUNCTION
    searchPrefix := trim(search.word);
    matches := sorted(wordIndex)(
                        keyed(wordIndex.word[1..length(searchPrefix)] = searchPrefix),
                        matchSingleWordFlags(wordIndex, search) AND
                        matchFirstWord(wordIndex, search) AND
                        matchWildcards(word, search.wildMask, search.wildMatch));

    return reduceMultipleToSingle(search, matches);
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

shared doSimpleReadDate(searchRecord search) := function
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

shared doSimpleTermAndTerm(searchRecord search, dataset(simpleCandidateRecord) in) := function
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    inRight := sort(in(stage = search.rightStage), doc);        // NB: change to sorted() not sort
    others := in(stage not in [search.leftStage, search.rightStage]);       // generates poor code: why?

    matches := join(inLeft, inRight, left.doc = right.doc, createSimpleCandidateTransform(search.stage, LEFT), INNER);
    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

shared doSimpleTermAndNotTerm(searchRecord search, dataset(simpleCandidateRecord) in) := function
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    inRight := sort(in(stage = search.rightStage), doc);        // NB: change to sorted() not sort
    others := in(stage not in [search.leftStage, search.rightStage]);       // generates poor code: why?

    matches := join(inLeft, inRight, left.doc = right.doc, createSimpleCandidateTransform(search.stage, LEFT), LEFT ONLY);
    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

shared doSimpleTermOrTerm(searchRecord search, dataset(simpleCandidateRecord) in) := FUNCTION
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    inRight := sort(in(stage = search.rightStage), doc);        // NB: change to sorted() not sort
    others := in(stage not in [search.leftStage, search.rightStage]);       // generates poor code: why?

    matches := join(inLeft, inRight, left.doc = right.doc, combineSimpleCandidateTransform(search.stage, LEFT, RIGHT), FULL OUTER);
    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

shared doSimpleTermAndWord(searchRecord search, dataset(simpleCandidateRecord) in) := function
    inLeft := sort(in(stage = search.leftStage), doc);          // NB: change to sorted() not sort
    others := in(stage != search.leftStage);

    matches := join(inLeft, wordIndex,
                    keyed(left.doc = right.doc) AND
                    matchSingleWord(RIGHT, search),
                    createSimpleCandidateTransform(search.stage, LEFT), INNER, keep(1));
    return others + matches;
END;

///////////////////////////////////////////////////////////////////////////////////////////////////////////

shared doSimpleTermAndDate(searchRecord search, dataset(simpleCandidateRecord) in) :=
    FAIL(recordof(in), 'Action doTermAndDate not yet implemented');

///////////////////////////////////////////////////////////////////////////////////////////////////////////

#option ('newChildQueries', true);
#option ('commonUpChildGraphs', true);
#option ('targetClusterType', 'roxie');

shared processStageSimple(searchRecord search, dataset(simpleCandidateRecord) in) :=
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

export SimpleQueryExecutor(dataset(searchRecord) queryDefinition, dataset(simpleCandidateRecord) initialResults) := module

    executionPlan := global(queryDefinition, few);          // Store globally for efficient access

    export internalResults := LOOP(initialResults, count(executionPlan), processStageSimple(executionPlan[NOBOUNDCHECK COUNTER], rows(left)));


    simpleUserOutputRecord :=
            record
    string  name{maxlength(MaxFilenameLength)};
            end;

    simpleUserOutputRecord createUserOutput(simpleCandidateRecord l) := transform
            SELF.name := docMetaIndex(doc = l.doc)[1].filename;
        END;

    export userOutput := project(internalResults, createUserOutput(left));

end;

export ExecuteReduceSimpleQuery(dataset(searchRecord) queryDefinition, dataset(simpleCandidateRecord) initialResults) := function

    return SimpleQueryExecutor(queryDefinition, initialResults).userOutput;
end;


export ExecuteSimpleQuery(dataset(searchRecord) queryDefinition) := function

    initialResults := dataset([], simpleCandidateRecord);

    return ExecuteReduceSimpleQuery(queryDefinition, initialResults);
end;

///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////


export ExecuteTwoPassQuery(dataset(searchRecord) queryDefinition) := function

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


//-------------------------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------------------------
//---------------------------------------- Test queries -------------------------------------------------------
//-------------------------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------------------------
export testing:=false;

#if (testing)



q1 := '"Gavin" and Hawthorn';
q2 := 'caps(gavin or jason) pre/3 hawthorn';
q3 := 'Abraham and not jesus';

q := q3;//'abraham and isaac';

parsed := parseLNQuery(q);
annotated := annotateQuery(parsed);
optimized := optimizeQuery(global(annotated, few));

output(q);
output(annotated);
output(optimized);
output(ExecuteFullQuery(global(optimized,few)));

#end

END;

/*
List of items requireing investigation:

2. Need a global(, few) to stop chaos.

4. Change number of docs to be a probablility
    - percolating it down is slightly tricky.  New ECL command?

5. Switch so query always uses the same candiate format - even for basic query.

6. Multiple shift actions of the same kind.

*/
