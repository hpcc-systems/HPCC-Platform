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

//skip type==setup TBD

//define constants
EXPORT TS := module

EXPORT MaxTerms             := 50;
EXPORT MaxStages            := 50;
EXPORT MaxProximity         := 10;
EXPORT MaxWildcard          := 1000;
EXPORT MaxMatchPerDocument  := 1000;
EXPORT MaxFilenameLength        := 255;
EXPORT MaxActions           := 255;
EXPORT MaxTagNesting        := 40;
EXPORT MaxColumnsPerLine := 10000;          // used to create a pseudo document position

EXPORT kindType         := enum(unsigned1, UnknownEntry=0, TextEntry, OpenTagEntry, CloseTagEntry, OpenCloseTagEntry, CloseOpenTagEntry);
EXPORT sourceType       := unsigned2;
EXPORT wordCountType    := unsigned8;
EXPORT segmentType      := unsigned1;
EXPORT wordPosType      := unsigned8;
EXPORT docPosType       := unsigned8;
EXPORT documentId       := unsigned8;
EXPORT termType         := unsigned1;
EXPORT distanceType     := integer8;
EXPORT indexWipType     := unsigned1;
EXPORT wipType          := unsigned8;
EXPORT stageType        := unsigned1;
EXPORT dateType         := unsigned8;

EXPORT sourceType docid2source(documentId x) := (x >> 48);
EXPORT documentId docid2doc(documentId x) := (x & 0xFFFFFFFFFFFF);
EXPORT documentId createDocId(sourceType source, documentId doc) := (documentId)(((unsigned8)source << 48) | doc);
EXPORT boolean      docMatchesSource(documentId docid, sourceType source) := (docid between createDocId(source,0) and (documentId)(createDocId(source+1,0)-1));

EXPORT wordType := string20;
EXPORT wordFlags    := enum(unsigned1, HasLower=1, HasUpper=2);

EXPORT wordIdType       := unsigned4;

EXPORT textSearchIndex  := index({ kindType kind, wordType word, documentId doc, segmentType segment, wordPosType wpos, indexWipType wip } , { wordFlags flags, wordType original, docPosType dpos}, '~DoesNotExist');
EXPORT wordIndexRecord := recordof(textSearchIndex);


END;
