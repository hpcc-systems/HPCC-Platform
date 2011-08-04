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

#option ('targetClusterType', 'hthor');
import ghoogle;
import lib_stringLib;

export sourceType       := ghoogle.sourceType;
export wordCountType    := ghoogle.wordCountType;
export segmentType      := ghoogle.segmentType;
export wordPosType      := ghoogle.wordPosType;
export docPosType       := ghoogle.docPosType;
export documentId       := ghoogle.documentId;
export termType         := ghoogle.termType;
export distanceType     := ghoogle.distanceType;
export stageType        := ghoogle.stageType;
export dateType         := ghoogle.dateType;
export charPosType      := ghoogle.charPosType;
export wordType         := ghoogle.wordType;
export wordFlags        := ghoogle.wordFlags;
export wordIdType       := ghoogle.wordIdType;
export corpusFlags      := ghoogle.corpusFlags;
export indexWipType     := ghoogle.indexWipType;

export NameCorpusIndex      := ghoogle.NameCorpusIndex;
export NameWordIndex        := ghoogle.NameWordIndex;
export NameSentenceIndex    := ghoogle.NameSentenceIndex;
export NameParagraphIndex   := ghoogle.NameParagraphIndex;
export NameDocMetaIndex     := ghoogle.NameDocMetaIndex;
export NameDateDocIndex     := ghoogle.NameDateDocIndex;
export NameDocPosIndex      := ghoogle.NameDocPosIndex;
export NameTokenisedDocIndex:= ghoogle.NameTokenisedDocIndex;
export NameTokenIndex       := ghoogle.NameTokenIndex;

documents := dataset([
{1, 'GENESIS.TXT'},
{1, 'EXODUS.TXT'},
{1, 'LEV.TXT'},
{1, 'NUM.TXT'},
{1, 'DEUT.TXT'},
{1, 'JOSHUA.TXT'},
{1, 'JUDGES.TXT'},
{1, 'RUTH.TXT'},
{1, '1SAM.TXT'},
{1, '2SAM.TXT'},
{1, '1KINGS.TXT'},
{1, '2KINGS.TXT'},
{1, '1CHRON.TXT'},
{1, '2CHRON.TXT'},
{1, 'EZRA.TXT'},
{1, 'ESTHER.TXT'},
{1, 'NEHEMIAH.TXT'},
{1, 'JOB.TXT'},
{1, 'PSALMS.TXT'},
{1, 'PROVERBS.TXT'},
{1, 'ECCL.TXT'},
{1, 'SONG.TXT'},
{1, 'ISAIAH.TXT'},
{1, 'JEREMIAH.TXT'},
{1, 'LAMENT.TXT'},
{1, 'EZEKIEL.TXT'},
{1, 'JOEL.TXT'},
{1, 'DANIEL.TXT'},
{1, 'OBADIAH.TXT'},
{1, 'AMOS.TXT'},
{1, 'HOSEA.TXT'},
{1, 'HAGGAI.TXT'},
{1, 'NAHUM.TXT'},
{1, 'HABBAKUK.TXT'},
{1, 'MICAH.TXT'},
{1, 'ZEPH.TXT'},
{1, 'JONAH.TXT'},
{1, 'ZECH.TXT'},
{1, 'MALACHI.TXT'},
{2, 'MATTHEW.TXT'},
{2, 'MARK.TXT'},
{2, 'LUKE.TXT'},
{2, 'JOHN.TXT'},
{2, 'ACTS.TXT'},
{2, 'ROMANS.TXT'},
{2, '2COR.TXT'},
{2, '1COR.TXT'},
{2, '2THES.TXT'},
{2, 'EPH.TXT'},
{2, 'PHILIP.TXT'},
{2, '1THES.TXT'},
{2, 'COL.TXT'},
{2, 'GAL.TXT'},
{2, '2TIM.TXT'},
{2, 'PHILEMON.TXT'},
{2, '1TIM.TXT'},
{2, 'TITUS.TXT'},
{2, 'HEBREWS.TXT'},
{2, 'JUDE.TXT'},
{2, '1PETER.TXT'},
{2, '2JOHN.TXT'},
{2, '2PETER.TXT'},
{2, '3JOHN.TXT'},
{2, 'JAMES.TXT'},
{2, '1JOHN.TXT'},
{2, 'REV.TXT'}], { unsigned2 source, string name{maxlength(100)} });

docMetaRecord := record
unsigned2           source;
unsigned8           doc;
string              name{maxlength(255)};
unsigned8           date;
unsigned8           size;
                end;

DirectoryPath := '~file::127.0.0.1::temp::asv2::';

p1 := project(documents,
            transform(docMetaRecord,
                    self.name := directoryPath+left.name;
                    self.doc := ghoogle.createDocId(left.source, 1);
                    self.date := (left.source-1)*4000 + 10*counter;     // any old values
                    self.size := 20000 - counter;                   // ditto
                    self := left;
                    self := []));

annotatedDocuments  := global(iterate(p1, transform(docMetaRecord, self.doc := if (left.source = right.source, left.doc+1, right.doc+1); self := right)), few);

parseRecord := record
wordType        word;
wordType        original;
documentId      doc;
segmentType     segment;
wordFlags       flags;
wordPosType     wpos;
indexWipType    wip;
docPosType      dpos;
boolean         beginSegment;           // could be set to {x:1} to separate title and chapters - would probably be useful.
boolean         endOfSentence;
boolean         endOfParagraph;
            end;



extractWords(unsigned8 doc, string name) := function


inFile := dataset(name, { string line{maxlength(10000)}, unsigned8 filepos{virtual(fileposition)} }, csv(separator('$$!!$$'), maxlength(10010)));

pattern patWord := pattern('[A-Za-z]+');
pattern patNumber := pattern('[0-9]+');
pattern sentenceTerminator := ['.','?'];
pattern punctuation := [',',';',':','(',')'];//,'"','\''];
pattern ws := [' ','\t'];
pattern verseRef := ('{' patNumber ':' patNumber '}');

pattern skipChars := ws | punctuation | verseRef;
pattern matchPattern := patWord | sentenceTerminator;
pattern S := skipChars* matchPattern;


parseRecord createMatchRecord(inFile l) := transform

        self.original := matchtext(patWord);
        self.doc := doc;
        self.dpos := l.filepos + matchposition(matchPattern)-1;
        self.wpos := 0;
        self.wip := 1;
        self.endOfSentence := matched(sentenceTerminator);
        self := [];
    END;

doProcess1 := parse(inFile, line, S, createMatchRecord(left), first, scan);


parseRecord createMatchPara(inFile l) := transform
        self.endOfParagraph := true;
        self.doc := doc;
        self.dpos := l.filepos;
        self := [];
    END;

pattern emptyLine := ws*;

doProcess2 := parse(inFile, line, emptyLine, createMatchPara(left), whole);

//return choosen(doProcess1, 100);
return doProcess1 + doProcess2;

end;

extractWordsFromDoc(docMetaRecord in) := extractWords(in.doc, in.name);



inputDatasetList := annotatedDocuments;

initial := dataset([], parseRecord);

extractAll := LOOP(initial, count(inputDatasetList), false, extractWordsFromDoc(inputDatasetList[counter]));
//extractAll := LOOP(initial, 2, false, extractWordsFromDoc(inputDatasetList[counter]));

//Assign word positions: - Sort because Sentence information isn't currently in correct order
sortedWords := sort(extractAll, doc, dpos);
prWords1 := group(sortedWords, doc);

//Sentance and paragraph both take up lexical space to stop phrases matching over a sentance/paragram boundary
prWords2 := group(iterate(prWords1, transform(parseRecord, self.wpos := left.wpos+left.wip; self := right)));
//prWords2 := group(iterate(prWords1, transform(parseRecord, self.wpos := if (left.endOfSentence or left.endOfParagraph, left.wpos, left.wpos+1); self := right)));

parseRecord cleanWords(parseRecord l) := transform
        hasUpper := REGEXFIND('[A-Z]', l.original);
        hasLower := REGEXFIND('[a-z]', l.original);
        self.word := StringLib.StringToLowerCase(l.original);
        self.flags := IF(hasUpper, wordFlags.hasUpper, 0) + IF(hasLower, wordFlags.hasUpper, 0);
        self := l;
    end;
prWords3 := project(prWords2(original <> ''), cleanWords(left));

processedWords := prWords3;
processedSentences := prWords2(endOfSentence);
processedParagraphs := prWords2(endOfParagraph);

//Generate the different indexes:
//corpusIndex       := index({ wordType word, wordIdType wordid, wordCountType numOccurences, wordCountType numDocuments, wordType normalized, corpusFlags flags } , NameCorpusIndex);
corpus1 := sort(processedWords, word, doc);
corpus2 := table(group(corpus1, word, doc, local), { word, wordIdType wordid := 0, wordCountType numOccurences := count(group), doc, wordType normalized := '', corpusFlags flags := 0 });
corpus3 := table(group(corpus2, word), { word, wordid, wordCountType numOccurences := sum(group, numOccurences), wordCountType numDocuments := count(group), normalized, flags });
BUILD(corpus3, { word, wordid, numOccurences, numDocuments, normalized, flags }, NameCorpusIndex, overwrite);

//wordIndex     := index({ wordType word, documentId doc, segmentType segment, wordFlags flags, wordType original, wordPosType wpos } , { docPosType dpos}, NameWordIndex);
//output(processedWords);
BUILD(processedWords, { word, doc, segment, wpos }, { wip, flags, original, dpos }, NameWordIndex, overwrite, compressed(row));

//SentenceIndex := index({ documentId doc, wordPosType wpos }, NameSentenceIndex);
BUILD(processedSentences, { doc, wpos }, NameSentenceIndex, overwrite);

//paragraphIndex    := index({ documentId doc, wordPosType wpos }, NameParagraphIndex);
BUILD(processedParagraphs, { doc, wpos }, NameParagraphIndex, overwrite);

//docMetaIndex  := index({ documentId doc }, { dateType date }, NameMetaIndex);
BUILD(inputDatasetList, { doc }, { filename := name, wordCountType numWords := 0, date, size }, NameDocMetaIndex, overwrite);

//dateDocIndex  := index({ dateType date }, { documentId doc }, NameDateIndex);
BUILD(inputDatasetList, { date }, { doc }, NameDateDocIndex, overwrite);

//docPosIndex       := index({ wordPosType wpos }, { docPosType dpos }, NameDocPosIndex);
BUILD(processedWords, { doc, wpos }, { dpos }, NameDocPosIndex, overwrite);

//tokenizedDocIndex := index({ documentId doc, wordPosType wpos}, { wordIdType wordid }, NameTokenisedDocIndex);

//tokenIndex        := index({ wordIdType wordid }, { wordType word, wordIdType normalized }, NameTokenIndex);

