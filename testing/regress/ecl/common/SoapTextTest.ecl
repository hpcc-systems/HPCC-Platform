/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.

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

//version multiPart=false,variant='default'

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
variant := #IFDEFINED(root.variant, '');

//--- end of version configuration ---

import $.^.setup;
files := setup.files(multiPart, false);

EXPORT SoapTextTest := MODULE

    import Std, Setup.Ts;
    EXPORT wordRec := { string word; };

    //External service 1: How many documents does this word appear in?
    EXPORT doDocumentCount(string search) := FUNCTION
        cleanedSearch := Std.Str.ToLowerCase(TRIM(search));
        searchIndex := files.getSearchIndexVariant(variant);
        matches := searchIndex(KEYED(kind = Ts.KindType.TextEntry AND word = cleanedSearch)) : onwarning(4523, ignore);
        bydocs := TABLE(matches, { cnt := COUNT(GROUP), doc }, doc);
        numDocs := COUNT(byDocs);
        RETURN numDocs;
    END;

    EXPORT countServiceRequest := RECORD
        STRING search;
    END;
    EXPORT countServiceResponse := RECORD
        UNSIGNED cnt;
    END;

    EXPORT documentCountService() := FUNCTION
        string searchWord := '' : stored('search');
        RETURN OUTPUT(ROW(TRANSFORM(countServiceResponse, SELF.cnt := doDocumentCount(searchWord))));
    END;

    //External searvice2: Given a list of words, find the documents they all occur in, and return the number of counts of each word
    EXPORT joinServiceResponseRecord := RECORD
        Ts.DocumentId doc;
        STRING word;
        UNSIGNED cnt;
    END;

    EXPORT doSearchWords(DATASET(wordRec) searchWords) := FUNCTION

        searchIndex := files.getSearchIndexVariant(variant);
        outputRecord := RECORDOF(searchIndex);

        doAction(SET OF DATASET(outputRecord) prev, UNSIGNED step) := FUNCTION
            searchWord := searchWords[step].word;
            cleanedSearch := Std.Str.ToLowerCase(TRIM(searchWord));
            matches := searchIndex(KEYED(kind = Ts.KindType.TextEntry AND word = cleanedSearch)) : onwarning(4523, ignore);
            doJoin := MERGEJOIN([prev[step-1], matches], STEPPED(LEFT.doc = RIGHT.doc), SORTED(doc));
            RETURN IF (step =1, matches, doJoin);
        END;

        nullInput := DATASET([], outputRecord);
        results := GRAPH(nullInput, count(searchWords), doAction(ROWSET(LEFT), COUNTER), PARALLEL);
        summary := TABLE(results, {doc, word, cnt := COUNT(GROUP)}, doc, word);
        RETURN PROJECT(summary, TRANSFORM(joinServiceResponseRecord, SELF.word := TRIM(LEFT.word), SELF := LEFT));
    END;

    EXPORT searchWordsService() := FUNCTION
        DATASET(wordRec) searchWords := DATASET([], wordRec) : stored('search');
        RETURN OUTPUT(doSearchWords(searchWords));
    END;

    EXPORT doMain(string serviceUrl, string searchWords, unsigned documentLimit) := FUNCTION

        soapcallDocumentCount(string searchWord) := SOAPCALL(serviceUrl, 'soaptest_getdocumentcount', countServiceRequest, transform(countServiceRequest, SELF.search := searchWord), countServiceResponse).cnt;
        callDocumentCount(string search) := IF((serviceUrl != ''), soapcallDocumentCount(search), doDocumentCount(search));

        soapcallSearchWords(DATASET(wordRec) searchWords) := SOAPCALL(serviceUrl, 'soaptest_getsearchwords', { DATASET(wordRec) search := searchWords }, DATASET(joinServiceResponseRecord));
        callSearchWords(DATASET(wordRec) searchWords) := IF((serviceUrl != ''), soapcallSearchWords(searchWords), doSearchWords(searchWords));

        splitWords := Std.Str.SplitWords(searchWords, ',', false);
        splitWordsDs := DATASET(splitwords, wordRec);

        wordsWithDocCounts := TABLE(splitWordsDs, { string word := word; numDocs := callDocumentCount(word); });
        //
        leastCommon := TOPN(wordsWithDocCounts, documentLimit, numDocs);

        searchAgain := PROJECT(leastCommon, TRANSFORM(wordRec, SELF.word := LEFT.word));
        joinLeastCommon := callSearchWords(searchAgain);

        rollupRecord := RECORD
            Ts.DocumentId doc;
            SET OF STRING words;
            SET OF UNSIGNED counts;
        END;

        rollupRecord rollupWords(joinServiceResponseRecord l, DATASET(joinServiceResponseRecord) matches) := TRANSFORM
            SELF.doc := l.doc;
            sortedWords := SORT(matches, -cnt);
            SELF.words := SET(sortedWords, word);
            SELF.counts := SET(sortedWords, cnt);
        END;

        rolledUp := ROLLUP(GROUP(joinLeastCommon, doc), GROUP, rollupWords(LEFT, ROWS(LEFT)));
        RETURN rolledUp;
    END;

    EXPORT mainService() := FUNCTION
        // The published search service take a list of words, and a maximum number of significant documents
        string searchWords := '' : stored('searchWords');
        unsigned documentLimit := 3 : stored('documentLimit');
        serviceUrl := '' : stored('url');
        unsigned maxResults := 50;

        RETURN OUTPUT(CHOOSEN(doMain(serviceUrl, searchWords, documentLimit), maxResults));
    END;

END;
