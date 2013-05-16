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

//UseStandardFiles
#option ('checkAsserts',false)

import Std.Str;
import Std.File AS FileServices;

rebuildSimpleIndex := true;
rebuildSearchIndex := setupTextFileLocation <> '';

DirectoryPath := '~file::' + setupTextFileLocation + '::';          // path of the documents that are used to build the search index

MaxDocumentLineLength := 50000;

inputDocumentRecord := record
unsigned2   source;
unsigned4   subdoc;
string text{maxlength(MaxDocumentLineLength)};
unsigned8 filepos := 0;
    end;

parseRecord := record
TS_wordType     word;
TS_kindType     kind;
TS_wordType     original;
TS_documentId   doc;
TS_segmentType  segment;
TS_wordFlags    flags;
TS_wordPosType  wpos;
TS_indexWipType wip;
TS_docPosType   dpos;
TS_docPosType   seq;
boolean         beginSegment;           // could be set to {x:1} to separate title and chapters - would probably be useful.
boolean         endOfSentence;
boolean         endOfParagraph;
            end;

inputAliasRecord := record
unsigned2   source;
unsigned4   subdoc;
TS_wordType word;
TS_wordPosType wpos;
TS_indexWipType wip;
    end;

spanTagSet := ['p','s'];
wordKindSortOrder(TS_kindType kind, TS_wordPosType wip, TS_wordType tag) :=
        MAP(kind = TS_kindType.OpenTagEntry and wip=0=>1,
            kind = TS_kindType.CloseTagEntry=>2,
            kind = TS_kindType.OpenTagEntry and wip<>0=>
                100+CASE(tag,'p'=>1,'s'=>2,3),                      // ensure sentences are contained within paragraphs
            1000);

convertDocumentStreamToTokens(dataset(inputDocumentRecord) inFile) := FUNCTION

    inFile extractLine(inFile l, unsigned c) := transform
            startPos := IF(c = 1, 1, Str.Find(l.text, '\n', c-1)+1);
            _endPos := Str.Find(l.text, '\n', c);
            endPos := if(_endPos = 0, length(l.text), _endPos-1);
            self.filepos := l.filepos + startPos-1;
            self.text := l.text[startPos..endPos];
            self := l;
        end;

    splitFile := normalize(inFile, Str.FindCount(left.text, '\n')+1, extractLine(left, counter));

    pattern patWord := pattern('[A-Za-z][A-Za-z0-9]*');
    pattern patNumber := pattern('[0-9]+');
    pattern sentenceTerminator := ['.','?'];
    pattern punctuation := [',',';',':','(',')'];//,'"','\''];
    pattern ws := [' ','\t'];
    pattern verseRef := ('{' patNumber ':' patNumber '}');
    pattern tag := PATTERN('[A-Za-z][A-Za-z0-9_]*');
    pattern openTag := '<' tag ws* '>';
    pattern closeTag := '<' '/' tag ws* '>';
    pattern openCloseTag := '<' tag ws* '/' '>';
    pattern anyTag := openTag | closeTag | openCloseTag;


    pattern skipChars := ws | punctuation | verseRef;
    pattern matchPattern := patWord | sentenceTerminator | anyTag;
    pattern S := skipChars* matchPattern;


    parseRecord createMatchRecord(inFile l) := transform

            self.kind := MAP(MATCHED(patWord)=>TS_kindType.TextEntry,
                             MATCHED(openTag)=>TS_kindType.OpenTagEntry,
                             MATCHED(closeTag)=>TS_kindType.CloseTagEntry,
                             MATCHED(openCloseTag)=>TS_kindType.OpenCloseTagEntry,
                             MATCHED(sentenceTerminator)=>TS_kindType.CloseOpenTagEntry,
                             TS_kindType.UnknownEntry);

            self.original := MAP(MATCHED(sentenceTerminator)=>'s',
                                 MATCHED(patWord)=>matchtext(patWord),
                                 matchText(tag));
            self.doc := TS_createDocId(l.source, l.subdoc);
            self.dpos := l.filepos + matchposition(matchPattern);
            self.wpos := 0;
            self.wip := IF(MATCHED(anyTag), 0, 1);              // normal tags don't consume space - may want to change
            self.endOfSentence := matched(sentenceTerminator);
            self := [];
        END;

    doProcess1 := parse(splitFile, text, S, createMatchRecord(left), first, scan);

    parseRecord createMatchPara(inFile l) := transform
            self.kind := TS_kindType.CloseOpenTagEntry;
            self.original := 'p';
            self.endOfParagraph := true;
            self.wip := 1;
            self.doc := TS_createDocId(l.source, l.subdoc);
            self.dpos := l.filepos+1;
            self := [];
        END;

    pattern emptyLine := ws*;

    doProcess2 := parse(splitFile, text, emptyLine, createMatchPara(left), whole);

    RETURN merge(sorted(doProcess1, doc, dpos), doProcess2, sorted(doc, dpos));
END;


processSentanceAndParagraphMarkers(dataset(parseRecord) extractedWords, set of string spanTagSet) := FUNCTION

    spanTags := dataset(spanTagSet, { Ts_wordType tag });               // special tags which span all the text

    //Now normalize sentance and paragraph markers into begin/end markers.  And add leading and trailing markers to each document.
    withoutMarkers := extractedWords(kind <> TS_kindType.CloseOpenTagEntry);
    markers := extractedWords(kind = TS_kindType.CloseOpenTagEntry);

    parseRecord modifyMarker(parseRecord l, TS_kindType kind) := transform
        SELF.kind := kind;
        SELF.wip := IF(kind = TS_kindType.OpenTagEntry, l.wip, 0);      // open may take up space, close dosen't take up space
        SELF := l;
    END;
    markerOpen := project(markers, modifyMarker(left, TS_kindType.OpenTagEntry));
    markerClose := project(markers, modifyMarker(left, TS_kindType.CloseTagEntry));

    groupedByDoc := group(extractedWords, doc);
    singlePerDoc := table(groupedByDoc, { doc, maxDocPos := max(group, dpos); });

    parseRecord createSpanTag(TS_documentId doc, TS_docPosType dpos, boolean isOpen, unsigned whichTag) := TRANSFORM
        SELF.kind := IF(isOpen, TS_kindType.OpenTagEntry, TS_kindType.CloseTagEntry);
        SELF.original := spanTagSet[whichTag];
        SELF.doc := doc;
        SELF.dpos := dpos;
        SELF.wip := IF(isOpen, 1, 0);
        SELF := [];
    END;
    implicitStarts := sorted(normalize(singlePerDoc, count(spanTags), createSpanTag(LEFT.doc, 0, true, COUNTER)), doc, dpos, kind);
    implicitEnds := normalize(singlePerDoc, count(spanTags), createSpanTag(LEFT.doc, LEFT.maxDocPos+1, false, count(spanTags)+1-COUNTER));

    //Combine non tags, with end,begin for sentance,paragraph and implicit begin sentance, end sentance etc. for whole document
    cleaned := MERGE(implicitStarts, markerOpen, withoutMarkers, markerClose, implicitEnds, sorted(doc, dpos, wordKindSortOrder(kind, wip, original)));
    RETURN cleaned;
END;


assignWordPositions(dataset(parseRecord) inFile) := FUNCTION
    //Assign word positions: - Sort because Sentence information isn't currently in correct order
    sortedWords := sort(inFile, doc, dpos, assert);
    prWords1 := group(sortedWords, doc);

    //Sentance and paragraph both take up lexical space to stop phrases matching over a sentance/paragram boundary
    RETURN group(iterate(prWords1, transform(parseRecord, self.wpos := left.wpos+left.wip; self.seq := left.seq + 1; self := right)));
END;

matchOpenCloseTags(dataset(parseRecord) _inFile) := FUNCTION

    inFile := sorted(_inFile, doc, segment, wpos, wip, assert);         // input file needs to be sorted in this way - fail if not true.

    //Now need to match up begin tags with end tags, work out the size of the tags, update the begin tags, and strip the end tags.
    //extract all close tags, group by doc, sort by name, sequence, then add a new sequence.

    closeTags1 := inFile(kind = TS_kindType.CloseTagEntry);

    //Now extract all the open tags, and do the same
    openTags1 := inFile(kind = TS_kindType.OpenTagEntry);

    ////////Version2 - using grouped process to match up open with close ///////////////////
    positionRecord := { TS_docPosType seq, TS_docPosType wpos };
    matchTagRecord := RECORD
        dataset(positionRecord) active{maxcount(TS_MaxTagNesting)}
    END;
    nullMatchTag := row(transform(matchTagRecord, self := []));

    processEntry(parseRecord l, matchTagRecord r) := module
        export parseRecord createLeft := TRANSFORM
            //thrown away if not a close, so no problem with weird assigns, but don't skip because we want to modify right
            //could possibly change the semantics of process
            openWpos := r.active[1].wpos;
            self.wpos := openWpos;
            self.wip := l.wpos - openWpos;
            self.seq := IF(l.kind = TS_kindType.OpenTagEntry, 0, r.active[1].seq);      // so we can remove them later
            self.kind := TS_kindType.OpenTagEntry;
            self := l;
        END;
        export matchTagRecord createRight := TRANSFORM
            SELF.active := IF(l.kind = TS_kindType.OpenTagEntry,
                                row(transform(positionRecord, self.seq := l.seq; self.wpos := l.wpos)) & r.active,
                                r.active[2..]);
        END;
    END;

    openClose := inFile(kind IN [TS_kindType.OpenTagEntry,TS_kindType.CloseTagEntry]);
    groupedOpenClose := group(openClose, doc, segment);
    sortedOpenClose := sort(groupedOpenClose, original, wpos, wordKindSortOrder(kind, wip, original));

    groupedOpenCloseByTag := group(sortedOpenClose, doc, segment, original);
    processed := process(groupedOpenCloseByTag, nullMatchTag, processEntry(LEFT,RIGHT).createLeft, processEntry(LEFT,RIGHT).createRight);
    openLocations := processed(seq != 0);

    //Now perform a lookup join with the open tags by sequence to associate the wpos
    fixedupOpenTags := JOIN(openTags1, openLocations,
                            LEFT.doc = RIGHT.doc and LEFT.seq = RIGHT.seq,
                            transform(parseRecord, self.wip := right.wip; self := left));
    sortedFixedupOpenTags := SORT(fixedupOpentags, doc, segment, wpos, wip);

    //Now combine the non tags with the fixed up tags
    fixedUp := merge(inFile(kind not in [TS_kindType.OpenTagEntry, TS_kindType.CloseTagEntry]), sortedFixedupOpenTags, sorted(doc, segment, wpos, wip));
    return fixedUp;
END;

convertDocumentToInversion(dataset(inputDocumentRecord) inputDocuments) := FUNCTION
    extractedWords := convertDocumentStreamToTokens(inputDocuments);
    cleaned := processSentanceAndParagraphMarkers(extractedWords, spanTagSet);
    withPositions := assignWordPositions(cleaned);
    orderedWords := matchOpenCloseTags(withPositions);
    return orderedWords;
END;

normalizeWordFormat(dataset(parseRecord) inFile) := FUNCTION
    parseRecord cleanWords(parseRecord l) := transform
            hasUpper := REGEXFIND('[A-Z]', l.original);
            hasLower := REGEXFIND('[a-z]', l.original);
            self.word := Str.ToLowerCase(l.original);
            self.flags := IF(hasUpper, TS_wordFlags.hasUpper, 0) + IF(hasLower, TS_wordFlags.hasLower, 0);
            self := l;
        end;
    RETURN project(inFile, cleanWords(left));
END;

createAliasesFromList(dataset(inputAliasRecord) inputAliases) := FUNCTION
    parseRecord createAlias(inputAliasRecord l) := transform
            self.kind := TS_kindType.TextEntry;
            self.original := l.word;
            self.doc := TS_createDocId(l.source, l.subdoc);
            self.dpos := 0;
            self.wpos := l.wpos;
            self.wip := l.wip;
            self := [];
            self := l;
    end;

    aliases := project(inputAliases, createAlias(LEFT));
    RETURN sort(aliases, doc, segment, wpos, wip);
END;



//********************************************************************************************
//** Code to generate the simple text searching index.
//********************************************************************************************

inputDocuments := dataset([
//source1 : nursery rhymes
{1, 1, 'One two three four five, once I caught a fish alive, six seven eight nine ten, then I let it go again.\n\n' +
       'Why did you let it go? Because it bit my finger so.  Which finger did it bite?  This little finger on the right.'},
{1, 2, '<name>Little Jack Horner</name>, sat in a corner, eating a christmas pie.  He stuck in his thumb and pulled out a plum, and said, "What a good boy am I!"'},
{1, 3, '<name>Little Bo Peep</name> has lost her sheep and can\'t tell where to find them.  Leave them alone, and they\'ll come home, wagging their tails behind them'},
{1, 4, '<name>Humpty Dumpty</name> sat on a wall.  <name>Humpty Dumpty</name> had a great fall.  All the king\'s horses and all the king\'s men couldn\'t put Humpty together again.'},
{1, 5, 'Baa, baa, black sheep, have you any wool?  Yes sir, yes sir, three bags full;  one for the master, and one for the dame, and one for the little boy who lives down the lane.'},
{1, 6, 'Twinkle, twinkle, little star, how I wonder what you are.  Up above the world so high, like a diamond in the sky.  Twinkle, twinkle, little star, how I wonder what you are.'},
{1, 7, 'Twinkle, twinkle, little bat! How I wonder what you\'re at! Up above the world you fly, like a teatray in the sky. Twinkle, twinkle, little bat! How I wonder what you\'re at!'},

//source2: proverbs
{2, 1, 'There\'s a black sheep in the family.'},
{2, 2, 'All\'s well that ends well.'},

//Special test cases
{3, 1, 'Sheep that are black or black spotted go baa baa'},     // 2 blacks to test skipping on and not
{3, 2, 'Pigs that are black go baa baa also'},
{3, 3, 'White sheep and black sheep go baa'},
{3, 4, 'What\'s black and white and read all over?'},
{3, 5, 'ONE'},
{3, 6, 'x y x y x x y x'},
{3, 7, 'stuart james arthur x'},
{3, 8, 'stuart james arthur stuart'},
{3, 9, 'pad pad ship sank pad pad sank ship pad pad sank pad ship pad pad ship pad sank pad pad'},
{3, 10, 'za zz zx za zx zb zc zd zx'},
{3, 11, 'International business machines are in the business!'},
{3, 12, 'What business has IBM messing with services?'},
{3, 13, 'International business machines IBM International business machines IBM'},

{4, 1, 'The door was open and out of the gate was the source of the noise.  Maybe we need some software to help'},
{4, 2, 'The open and source needs software to help,  but is the source open enough?'},
{4, 3, 'The open source model may work for some software.  However open source does not always work'},
{4, 4, 'We generally are positive about software from an open source perspective'},
{4, 5, 'Should we use open source software, or proprietary?  I think open source software'},
{4, 6, 'This is about cheese, stilton, brie, and of course wensleydale'},
{4, 7, 'Where shall we source sofware from?  I am open to suggestions. Maybe that is the source of the problem'},
{4, 8, 'Many open source projects fail because the open source software is not sufficiently robust'},

{5, 1, '<box>cat dog <suitcase>shirt sock sock jacket trouser</suitcase><box>train train<suitcase>dress blouse hairdrier tights sock</suitcase></box>car</box>lego computer<box>pen paper glue</box>'},
{5, 2, '<socks>fox</socks>. knox on <box><socks>fox</socks></box> Socks on <box>knox</box>.'},          // apologies to Dr Seuss

{5, 3, 'one two three four <range1>five six<range2>seven eight</range2>nine ten</range1> eleven twelve'},
{5, 4, 'one two three four <range1>five six<range2>seven eight</range1>nine ten</range2> eleven twelve'},
{5, 5, 'one two <range1>three four five</range1> <range2>six seven eight nine ten eleven</range2> twelve'},
{5, 6, 'one two <range1>three four five</range1> six seven <range2>eight nine ten eleven</range2> twelve'},

{5, 7, 'little <name> little john </name > little james.'},

//Useful for testing how well the dynamic skipping works - e.g., the median calulations
{6, 0x00, '' },
{6, 0x01, 'gch01' },
{6, 0x02, 'gch02' },
{6, 0x03, 'gch01 gch02' },
{6, 0x04, 'gch04' },
{6, 0x05, 'gch01 gch04' },
{6, 0x06, 'gch02 gch04' },
{6, 0x07, 'gch01 gch02 gch04' },
{6, 0x08, 'gch08' },
{6, 0x09, 'gch01 gch08' },
{6, 0x0a, 'gch02 gch08' },
{6, 0x0b, 'gch01 gch02 gch08' },
{6, 0x0c, 'gch04 gch08' },
{6, 0x0d, 'gch01 gch04 gch08' },
{6, 0x0e, 'gch02 gch04 gch08' },
{6, 0x0f, 'gch01 gch02 gch04 gch08' },
{6, 0x10, 'gch10' },
{6, 0x11, 'gch01 gch10' },
{6, 0x12, 'gch02 gch10' },
{6, 0x13, 'gch01 gch02 gch10' },
{6, 0x14, 'gch04 gch10' },
{6, 0x15, 'gch01 gch04 gch10' },
{6, 0x16, 'gch02 gch04 gch10' },
{6, 0x17, 'gch01 gch02 gch04 gch10' },
{6, 0x18, 'gch08 gch10' },
{6, 0x19, 'gch01 gch08 gch10' },
{6, 0x1a, 'gch02 gch08 gch10' },
{6, 0x1b, 'gch01 gch02 gch08 gch10' },
{6, 0x1c, 'gch04 gch08 gch10' },
{6, 0x1d, 'gch01 gch04 gch08 gch10' },
{6, 0x1e, 'gch02 gch04 gch08 gch10' },
{6, 0x1f, 'gch01 gch02 gch04 gch08 gch10' },
{6, 0x20, 'gch20' },
{6, 0x21, 'gch01 gch20' },
{6, 0x22, 'gch02 gch20' },
{6, 0x23, 'gch01 gch02 gch20' },
{6, 0x24, 'gch04 gch20' },
{6, 0x25, 'gch01 gch04 gch20' },
{6, 0x26, 'gch02 gch04 gch20' },
{6, 0x27, 'gch01 gch02 gch04 gch20' },
{6, 0x28, 'gch08 gch20' },
{6, 0x29, 'gch01 gch08 gch20' },
{6, 0x2a, 'gch02 gch08 gch20' },
{6, 0x2b, 'gch01 gch02 gch08 gch20' },
{6, 0x2c, 'gch04 gch08 gch20' },
{6, 0x2d, 'gch01 gch04 gch08 gch20' },
{6, 0x2e, 'gch02 gch04 gch08 gch20' },
{6, 0x2f, 'gch01 gch02 gch04 gch08 gch20' },
{6, 0x30, 'gch10 gch20' },
{6, 0x31, 'gch01 gch10 gch20' },
{6, 0x32, 'gch02 gch10 gch20' },
{6, 0x33, 'gch01 gch02 gch10 gch20' },
{6, 0x34, 'gch04 gch10 gch20' },
{6, 0x35, 'gch01 gch04 gch10 gch20' },
{6, 0x36, 'gch02 gch04 gch10 gch20' },
{6, 0x37, 'gch01 gch02 gch04 gch10 gch20' },
{6, 0x38, 'gch08 gch10 gch20' },
{6, 0x39, 'gch01 gch08 gch10 gch20' },
{6, 0x3a, 'gch02 gch08 gch10 gch20' },
{6, 0x3b, 'gch01 gch02 gch08 gch10 gch20' },
{6, 0x3c, 'gch04 gch08 gch10 gch20' },
{6, 0x3d, 'gch01 gch04 gch08 gch10 gch20' },
{6, 0x3e, 'gch02 gch04 gch08 gch10 gch20' },
{6, 0x3f, 'gch01 gch02 gch04 gch08 gch10 gch20' },

{99,999, ''}
], inputDocumentRecord);


//MORE: This should be a set of aliases, which are then matched into the document automatically....
//e.g.,
//{['International','Business','Machines','IBM']},
//{['Twinkle','Twinkle','little','star','TTLS']},
//{['Twinkle','Twinkle','little','bat','TTLB']},
//would require some interesting processing though - effectively need to build a state table, tag the last matching entry,
//then split, introduce a new term, sort, and merge back in.

//A dataset to provide a set of aliases/abreviations to aid searching.  These typically have wip>1
inputAliases := dataset([
//NB: Entries must be in (source, doc, wpos, wip order), not initial sentance and paragraph markers take up two word positions.
{3,11,'IBM',2,3},
{3,13,'IBM',2,3},
{3,13,'IBM',6,3}
], inputAliasRecord);

orderedWords := convertDocumentToInversion(inputDocuments);
orderedAliases := createAliasesFromList(inputAliases);          //This should really be created dynamically from a list of tokens in the input dataset

wordsAndAliases := merge(orderedWords, orderedAliases, sorted(doc, segment, wpos, wip));

normalizedInversion := normalizeWordFormat(wordsAndAliases);

#if (useLocal=true)
  processedWords := DISTRIBUTE(normalizedInversion, IF(doc > 6, 0, 1));
#else
  processedWords := normalizedInversion;
#end

doCreateSimpleIndex() := sequential(
    BUILD(processedWords, { kind, word, doc, segment, wpos, wip }, { flags, original, dpos }, TS_NameWordIndex, overwrite,
#if (useLocal=true)
            NOROOT,
#end
            compressed(row)),
    //Add a column mapping, testing done L->R, multiple transforms, and that parameters work
    fileServices.setColumnMapping(TS_NameWordIndex, 'word{set(stringlib.StringToLowerCase,stringlib.StringFilterOut(\'AEIOU$?.:;,()\'))}')
);


//********************************************************************************************
//** Code to generate a new
//********************************************************************************************

boolean isNewDocumentFunction(string s) := false;

convertTextFileToInversion(TS_sourceType sourceId, string filename, isNewDocumentFunction isNewDocument) := FUNCTION

inFile := dataset(filename, { string line{maxlength(MaxDocumentLineLength)}, unsigned8 filepos{virtual(fileposition)} }, csv(SEPARATOR(''),quote([]),maxlength(MaxDocumentLineLength+8+4)));

    inputDocumentRecord createInputRecord(inFile l) := TRANSFORM
        self.source := sourceId;
        self.subdoc := 0;
        self.text := l.line;
        self.filepos := l.filepos;
    END;
    annotateWithSource := PROJECT(inFile, createInputRecord(LEFT));

    inputDocumentRecord allocateDocuments(inputDocumentRecord l, inputDocumentRecord r) := transform
        SELF.subdoc := IF(l.subdoc = 0, 1, l.subdoc + IF(isNewDocument(r.text), 1, 0));
        //This turns dpos into a (line, column) field instead of byte offset.  Remove for the latter.
        SELF.filepos := l.filepos + TS_MaxColumnsPerLine;
        SELF := r;
    END;
    annotateWithDocument := ITERATE(annotateWithSource, allocateDocuments(LEFT, RIGHT));
    startOfEachDocument := ITERATE(annotateWithDocument, TRANSFORM(inputDocumentRecord, SKIP(left.subdoc = RIGHT.subdoc), SELF:= RIGHT));
    inversion := convertDocumentToInversion(annotateWithDocument);
    RETURN inversion;
END;

boolean splitBibleBook(string s) := function
    RETURN REGEXFIND('^(' +
        'THE (FIRST |SECOND |THIRD |FOURTH )?BOOK[E]? OF|'+
        'ECCLESIASTES|' +
        'SOLOMON\'S CANTICLE OF CANTICLES|' +
        'ECCLESIASTICUS|' +
        'THE PROPHECY OF|' +
        'THE LAMENTATIONS OF JEREMIAS|' +
        'THE HOLY GOSPEL OF|' +
        'THE ACTS OF|' +
        'THE ([A-Z]+ )?EPISTLE|' +
        'THE APOCALYPSE OF' +
        ')|([0-9]+:[0-9]+)', s);
END;

boolean splitEncyclopedia(string s) := REGEXFIND('^A[A-Z]+ ', s);               // poor, but good enough for our purposes.
boolean splitShakespeare(string s) := REGEXFIND('^1[56][0-9][0-9]', s);
boolean splitDonQuixote(string s) := REGEXFIND('^CHAPTER', s);

bibleStream := normalizeWordFormat(convertTextFileToInversion(1, DirectoryPath+'0drvb10.txt', splitBibleBook));
encyclopediaStream := normalizeWordFormat(convertTextFileToInversion(2, DirectoryPath+'pge0112.txt', splitEncyclopedia));
donQuixoteStream := normalizeWordFormat(convertTextFileToInversion(3, DirectoryPath+'donQuixote.txt', splitDonQuixote));
shakespeareStream := normalizeWordFormat(convertTextFileToInversion(4, DirectoryPath+'shaks12.txt', splitShakespeare));         // not convinced we can use this


//Build on bible and encyclopedia for the moment.
//have different characteristics.  Bible has ~74 "documents", encyclopedia has
doCreateSearchIndex() := sequential(
    BUILD(bibleStream+encyclopediaStream, { kind, word, doc, segment, wpos, wip }, { flags, original, dpos }, TS_NameSearchIndex, overwrite,
#if (useLocal=true)
        NOROOT,
#end
          compressed(row)),
    fileServices.setColumnMapping(TS_NameSearchIndex, 'word{set(unicodelib.UnicodeToLowerCase)}')       // unicode just to be perverse
);


IF (rebuildSimpleIndex, doCreateSimpleIndex());
IF (rebuildSearchIndex, doCreateSearchIndex());
