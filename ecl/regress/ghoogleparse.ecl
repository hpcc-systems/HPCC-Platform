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

ghoogle.ghoogleDefine();


productionRecord  :=
            record
dataset(searchRecord) actions;
            end;

unknownTerm := (termType)-1;

PRULE := rule type (productionRecord);
ARULE := rule type (searchRecord);

CmdDummy() := transform(searchRecord, self := []);
CmdSimple(actionEnum action) := transform(searchRecord, self.action := action; self := []);

///////////////////////////////////////////////////////////////////////////////////////////////////////////

pattern ws := [' ','\t'];

token number    := pattern('[0-9]+');
token word      := pattern('[A-Za-z]+');
//token date := pattern('[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}');
token date      := pattern('[0-9][0-9]?/[0-9][0-9]?/[0-9][0-9]([0-9][0-9])?');
token wildcarded := pattern('[A-Z*a-z]+');
token suffixed  := pattern('[A-Za-z]+!');

token atleast   := 'ATLEAST' pattern('[0-9]+');
token capsBra   := 'CAPS(';
token noCapsBra := 'NOCAPS(';

token quoteChar := '"';

ProdProximityFilter(boolean preceeds, unsigned distance) := CmdProximityFilter(0, [], [], preceeds, distance);

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
    := 'PRE/'                                   ProdProximityFilter(true, 0)
    |  pattern('PRE/[0-9]+')                    ProdProximityFilter(true, (integer)$1[5..])
    |  'PRE/P'                                  CmdParagraphFilter(0, 0, 0, true)
    |  'PRE/S'                                  CmdSentanceFilter(0, 0, 0, true)
    |  pattern('/[0-9]+')                       ProdProximityFilter(false, (integer)$1[2..])
    |  '/P'                                     CmdParagraphFilter(0, 0, 0, false)
    |  '/S'                                     CmdSentanceFilter(0, 0, 0, false)
    ;

rule numericValue
    := number
    |  '$' number
    |  number 'mm'
    ;

rule range
    := numericValue '..' numericValue
    ;

rule forwardExpr := use(expr);

rule segmentName
//  := word not in ['CAPS','NOCAPS','ALLCAPS']
    := validate(word, matchtext not in ['CAPS','NOCAPS','ALLCAPS']);
    ;

ARULE term0
    := word                                     CmdReadWord(unknownTerm, 0, 0, $1, 0, 0)
    |  suffixed                                 CmdReadWildWord(unknownTerm, 0, 0, $1[1..length(trim($1))-1], '', '')
    |  wildcarded                               CmdDummy()
    |  range                                    CmdDummy()
    |  '(' forwardExpr ')'                      CmdDummy()
    | 'CAPS' '(' forwardExpr ')'                transform(searchRecord,
                                                    self.action := actionEnum.FlagModifier;
                                                    self.wordFlagMask := wordFlags.HasLower+wordFlags.HasUpper,
                                                    self.wordFlagCompare := wordFlags.HasUpper,
                                                    self := []
                                                    )
    | 'NOCAPS' '(' forwardExpr ')'              CmdDummy()
    | 'ALLCAPS' '(' forwardExpr ')'             CmdDummy()
    | segmentName '(' forwardExpr ')'           CmdDummy()
    | atleast '(' forwardExpr ')'               CmdDummy()
    ;

PRULE phrase
    :=  term0                                   transform(productionRecord, self.actions := dataset($1))
    |   SELF term0                              transform(productionRecord, self.actions := $1.actions + dataset($2))
    ;


rule condition
    := word compare number
    |  word compare date
    ;

PRULE term
    :=  phrase
    |   quoteChar phrase quoteChar              transform(productionRecord, self.actions := $2.actions + row(CmdSimple(actionEnum.QuoteModifier)))
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
    := combined
    ;

infile := dataset([
        {'gavin and hawthorn'},
        {'(gavin or jason) pre/3 hawthorn'},
        {''}
        ], { string line });

resultsRecord :=
        record(recordof(infile))
dataset(searchRecord) actions;
        end;


resultsRecord extractResults(infile l, dataset(searchRecord) actions) :=
        TRANSFORM
            SELF := l;
            SELF.actions := actions;
        END;

output(PARSE(infile,line,expr,extractResults(LEFT, $1.actions),first,whole,skip(ws),parse));







//-----------------------------------------------------------------------------------------------

//processing

