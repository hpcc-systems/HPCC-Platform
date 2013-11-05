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

import Std.Str;

export jlib:= SERVICE
    unsigned8 rtlTick() : library='jlib',eclrtl,entrypoint='rtlNano';
END;

TextRecord := RECORD
    string line{maxlength(4096)};
end;


TextData_In := DATASET([{'The Florida Marlins went quickly from a team of uncertain future to a team with a strong foundation and positive outlook. After months of failing efforts, Owner John Henry finally found a way to build a 40,000 seat convertible roof stadium, for a price close to $385 million. The plan calls for a significant contribution on behalf of John Henry, but will include help from a Florida bed tax. In addition, the Marlins have signed a 40 year lease to use the facility, which will be built in Bicentennial Park in downtown Miami. The stadium will be finished for the 2004 season, the same year that the Marlins will officially become the Miami Marlins, pending league approval. A day after the monumental announcement, former Marlins catcher and Series hero Charles Johnson was signed, making a return to the team that gave him his start. The Marlins signed Johnson to a 5 year contract, worth an estimated $35 million. Johnson is coming off a career year, and should provide the Fish with solid veteran leadership.\014'},
                        {'MIAMI -- The Florida Marlins addressed their need for an outfielder with left-handed pop, signing former Colorado Rockies outfielder Todd Hollandsworth of Colorado to a one-year, $1.5 million contract Wednesday'},
                        {'Hollandsworth hit .284 last season with 16 home runs and 67 RBI in 134 games for Colorado and Texas. His signing fills the void created when the Marlins did not offer a contract to outfielder Eric Owens, who later signed with the Anaheim Angels.'}],
                        TextRecord);

// Move Data into record with docID and line_num fields
TextDocIDRecord := RECORD
    STRING line{MAXLENGTH(4096)} := TextData_In.line;
    INTEGER line_num := 0;
    INTEGER docID := 0;
END;

// TODO: could try higher num
TextData := TABLE(TextData_In, TextDocIDRecord);

// Create Document IDs per Form Feed
TextDocIDRecord GenerateDocIDs (TextDocIDRecord L, TextDocIDRecord R) := 
TRANSFORM
        SELF.docID := IF (Str.Find(L.line, '\014', 1) <> 0 OR L.docID = 0, L.docID + 1, L.docID);
        SELF.line_num := IF (Str.Find(L.line, '\014', 1) <> 0 OR L.docID = 0, 1, L.line_num + 1);
        SELF := R;
END;

export Docs_by_DocNum := ITERATE (TextData, GenerateDocIDs(LEFT, RIGHT));


export PATTERN AlphaUpper := PATTERN('[A-Z]');
export PATTERN AlphaLower := PATTERN('[a-z]');
export pattern Alpha := PATTERN('[A-Za-z]');

// Flags for type of match
Flag_FullMatch := 2;
Flag_PartialMatch := 1;
Flag_NoMatch := -1;

// Create Pattern for name
BOOLEAN isCommon(STRING test) := test IN ['A', 'An', 'The'];
token NotCommon(token Test) := VALIDATE(Test, NOT isCommon(MATCHTEXT)); 
token Blanks := ([' ','\r','\n','\t'])+;
pattern Abbr := alphaupper '.';
pattern Title := ('Mr' | 'Ms' | 'Mrs' | 'Miss') OPT('.');
token OptNames := NotCommon(alphaupper alphalower+) | Abbr;
RULE FirstN := OptNames;
RULE MiddleN := OptNames repeat(Blanks OptNames, 0, 4);
// TODO: include other prefixes
pattern alphaplus := alpha+;
RULE Prefix := (alphaplus '-') |
               'bin ' | 'Bin ';
// nb allowing for upper or lower in last name
RULE LastN := NotCommon(alphaupper alpha+);
RULE FullName := (LastN ',' Blanks FirstN OPT(Blanks MiddleN)) 
              | (OPT(Title Blanks) OPT(FirstN Blanks OPT(MiddleN Blanks)) OPT(Prefix) LastN)
              ;
// Create Pattern for an association
token ConnectWord := ['with','of','for','at'];
rule Connector := (Blanks ConnectWord) | (':');
RULE MultiNames := OptNames repeat(Blanks OptNames, 0, 4);
//RULE MultiNames := OptNames (Blanks OptNames)*;
RULE Associated := Connector Blanks MultiNames;
RULE FullNameAssoc := FullName OPT(Associated);

//startTime := jlib.rtlTick() : stored('startTime');

// Find all Full Names and Partial Names
Matches := 
    record
        //string tree2      := parseLib.getXmlParseTree()+'\n';
        string txt          := '\''+MATCHTEXT+'\':';
        STRING fname        := MATCHTEXT(FirstN)+',';
        STRING mname        := MATCHTEXT(MiddleN)+',';
        STRING prefx        := MATCHTEXT(Prefix)+',';
        STRING lname        := MATCHTEXT(LastN)+',';
        STRING associate    := MATCHTEXT(Associated / MultiNames)+',';
        INTEGER  doc        := Docs_by_DocNum.docID;
        INTEGER  line_num   := Docs_by_DocNum.line_num;
        INTEGER  MatchPos   := MATCHPOSITION(FullName);
        INTEGER  MatchLen   := MATCHLENGTH(FullName);
        INTEGER  numOfRefs  := 1;
        INTEGER  nameScore  := 0;
        INTEGER  matchScore := 0;
        INTEGER1 isPartial  := 0;
        unsigned4 allLength := MATCHLENGTH;
//      unsigned8 tick := jlib.rtlTick() - startTime;   
    end;

infile := choosen(Docs_by_DocNum,1000);

output(sort(PARSE(infile,line, FullNameAssoc, Matches, scan,parse),doc,line_num,matchPos,allLength));

output(PARSE(Docs_by_DocNum,line, FullNameAssoc, Matches, scan all,parse),named('AllMatches'));                 // all matches
output(PARSE(Docs_by_DocNum,line, FullNameAssoc, Matches, max,scan,parse),named('LongestMatch'));                   // longest match
output(PARSE(Docs_by_DocNum,line, FullNameAssoc, Matches, many max,scan,parse),named('LongestManyNoOverlap'));          // longest non-overlapping matches
output(PARSE(Docs_by_DocNum,line, FullNameAssoc, Matches, many max,scan all,parse),named('LongestManyOverlap'));        // longest matches, overlapping

