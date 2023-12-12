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

    EXPORT ConnectionType := ENUM(Plain, Encrypted, Trusted);

    EXPORT CallOptionsRecord := RECORD
        ConnectionType connectionToRoxie := ConnectionType.Encrypted;
        ConnectionType connectionToEsp := ConnectionType.Encrypted;
        boolean persistConnectToRoxie := FALSE;
        boolean persistConnectToEsp := FALSE;
        boolean connectDirectToRoxie := FALSE;
        boolean embedServiceCalls := FALSE;
    END;

    EXPORT ConfigOptionsRecord := RECORD
        string remoteEspUrl := '';
        string remoteRoxieUrl := '';
        string RoxieUrl_Plain := '';
        string RoxieUrl_Encrypted := '';
        string RoxieUrl_Trusted := '';
        string EspUrl_Plain := '';
        string EspUrl_Encrypted := '';
        string EspUrl_Trusted := '';
    END;

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
        CallOptionsRecord callOptions;
    END;
    EXPORT countServiceResponse := RECORD
        UNSIGNED cnt;
    END;

    EXPORT documentCountService() := FUNCTION
        string searchWord := '' : stored('search');
        RETURN OUTPUT(ROW(TRANSFORM(countServiceResponse, SELF.cnt := doDocumentCount(searchWord))));
    END;

    //External searvice2: Given a list of words, find the documents they all occur in, and return the number of counts of each word
    EXPORT JoinServiceRequestRecord := RECORD
        DATASET(wordRec) search;
        CallOptionsRecord callOptions;
    END;

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

    EXPORT MainServiceRequestRecord := RECORD
        string searchWords := '';
        unsigned documentLimit := 3;
        string serviceUrl := '';
        unsigned maxResults := 50;

        CallOptionsRecord callOptions;
        ConfigOptionsRecord configOptions;
    END;

    //The response from the main service
    EXPORT MainServiceResponseRecord := RECORD
        Ts.DocumentId doc;
        SET OF STRING words;
        SET OF UNSIGNED counts;
    END;

    EXPORT doMain(string serviceUrl, string searchWords, unsigned documentLimit, CallOptionsRecord callOptions, ConfigOptionsRecord configOptions) := FUNCTION

        boolean usePersistConnection := IF(callOptions.connectDirectToRoxie, callOptions.persistConnectToRoxie, callOptions.persistConnectToEsp);
        unsigned persistConnectCount := IF(usePersistConnection, 100, 0);

        soapcallDocumentCount(string searchWord) := SOAPCALL(serviceUrl, 'soaptest_getdocumentcount', countServiceRequest, transform(countServiceRequest, SELF.search := searchWord, SELF.callOptions := callOptions), countServiceResponse, PERSIST(persistConnectCount),timeout(1)).cnt;
        callDocumentCount(string search) := IF(NOT callOptions.embedServiceCalls, soapcallDocumentCount(search), doDocumentCount(search));

        soapcallSearchWords(DATASET(wordRec) searchWords) := SOAPCALL(serviceUrl, 'soaptest_getsearchwords', JoinServiceRequestRecord, transform(JoinServiceRequestRecord, SELF.search := searchWords, SELF.callOptions := callOptions), DATASET(joinServiceResponseRecord), PERSIST(persistConnectCount),timeout(1));
        callSearchWords(DATASET(wordRec) searchWords) := IF(NOT callOptions.embedServiceCalls, soapcallSearchWords(searchWords), doSearchWords(searchWords));

        splitWords := Std.Str.SplitWords(searchWords, ',', false);
        splitWordsDs := DATASET(splitwords, wordRec);

        wordsWithDocCounts := TABLE(splitWordsDs, { string word := word; numDocs := callDocumentCount(word); });
        //
        leastCommon := TOPN(wordsWithDocCounts, documentLimit, numDocs);

        searchAgain := PROJECT(leastCommon, TRANSFORM(wordRec, SELF.word := LEFT.word));
        joinLeastCommon := callSearchWords(searchAgain);

        MainServiceResponseRecord rollupWords(joinServiceResponseRecord l, DATASET(joinServiceResponseRecord) matches) := TRANSFORM
            SELF.doc := l.doc;
            sortedWords := SORT(matches, -cnt);
            SELF.words := SET(sortedWords, word);
            SELF.counts := SET(sortedWords, cnt);
        END;

        rolledUp := ROLLUP(GROUP(joinLeastCommon, doc), GROUP, rollupWords(LEFT, ROWS(LEFT)));
        RETURN rolledUp;
    END;

    // The published search service take a list of words, and a maximum number of significant documents
    EXPORT runMainService(string searchWords, unsigned documentLimit, unsigned maxResults, CallOptionsRecord callOptions, ConfigOptionsRecord configOptions) := FUNCTION

        defaultRoxieUrl := IF(configOptions.remoteRoxieUrl != '', configOptions.remoteRoxieUrl, '.');
        defaultEspUrl := IF(configOptions.remoteEspUrl != '', configOptions.remoteEspUrl, '.');
        plainRoxieUrl := IF(configOptions.RoxieUrl_Plain != '', configOptions.RoxieUrl_Plain, defaultRoxieUrl + ':9876');
        encryptedRoxieUrl := IF(configOptions.RoxieUrl_Encrypted != '', configOptions.RoxieUrl_Encrypted, defaultRoxieUrl + ':19876');
        trustedRoxieUrl := IF(configOptions.RoxieUrl_Trusted != '', configOptions.RoxieUrl_Trusted, defaultRoxieUrl + ':29876');
        plainEspUrl := IF(configOptions.EspUrl_Plain != '', configOptions.EspUrl_Plain, defaultEspUrl + ':8002//WsEcl/soap/query/roxie');
        encryptedEspUrl := IF(configOptions.EspUrl_Encrypted != '', configOptions.EspUrl_Encrypted, defaultEspUrl + ':18002//WsEcl/soap/query/roxie');
        trustedEspUrl := IF(configOptions.EspUrl_Trusted != '', configOptions.EspUrl_Trusted, defaultEspUrl + ':28002//WsEcl/soap/query/roxie ');

        roxieUrl := CASE(callOptions.connectionToRoxie,
                         ConnectionType.Plain =>     'http://' + plainRoxieUrl,
                         ConnectionType.Encrypted => 'https://' + encryptedRoxieUrl,
                                                     'https://' + trustedRoxieUrl);
        espUrl := CASE(callOptions.connectionToEsp,
                         ConnectionType.Plain =>     'http://' + plainEspUrl,
                         ConnectionType.Encrypted => 'https://' + encryptedEspUrl,
                                                     'https://' + trustedEspUrl);
        serviceUrl := IF(callOptions.connectDirectToRoxie, roxieUrl, espUrl);

        RETURN OUTPUT(CHOOSEN(doMain(serviceUrl, searchWords, documentLimit, callOptions, configOptions), maxResults));
    END;

    EXPORT mainService() := FUNCTION
        // The published search service take a list of words, and a maximum number of significant documents
        string searchWords := '' : stored('searchWords');
        unsigned documentLimit := 3 : stored('documentLimit');
        unsigned maxResults := 50;

        callOptions := ROW(CallOptionsRecord) : stored('callOptions');
        configOptions := ROW(ConfigOptionsRecord) : stored('configOptions');

        RETURN runMainService(searchWords, documentLimit, maxResults, callOptions, configOptions);
    END;
END;
