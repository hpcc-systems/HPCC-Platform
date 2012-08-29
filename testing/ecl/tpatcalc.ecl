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

/*

This example demonstrates the use of productions inside parse statements.  Currently only supported in the tomita version of the parse statement.

It includes a use of the MATCHROW() operator to extract the attributes from a particular matched entry.  No really recomended, but it's there if you need it.

*/


pattern ws := [' ','\t'];
token number := pattern('[0-9]+');
token plus := '+';
token minus := '-';

attrRecord :=
        record
integer     value;
        end;

rule(attrRecord) e0 
        // following option expression is werid, and just to test generation of implicit transforms.
        // argument is optional => a default clear is created
        // transform is omitted, but a single input symbol has a compatible record type so implicit $$=$2 created
        := '(' use(attrRecord,expr)? ')'            //implicit  transform($2)
        |   number                                  transform(attrRecord, self.value := (integer)$1;)
        |   '-' SELF                                transform(attrRecord, self.value := -$2.value;)
        ;

rule(attrRecord) e1
        := e0
        |  SELF '*' e0                              transform(attrRecord, self.value := $1.value * $3.value;)
        |  use(attrRecord, e1) '/' e0               transform(attrRecord, self.value := $1.value / $3.value;)
        ;

rule(attrRecord) e2
        := e1
        |  self plus e1                             transform(attrRecord, self.value := $1.value + $3.value;)
        |  self minus e1                            transform(attrRecord, self.value := $1.value - $3.value;)
        ;

rule(attrRecord) expr 
        := e2
        ;

infile := dataset([
        {'1+2*3'},
        {'1+2*z'},
        {'1+2+(3+4)*4/2'},
        {'1*2+3*-2'},
        {'10'},
        {'10*()+4'},        // test default transform not standard syntax!
        {''}
        ], { string line });

resultsRecord := 
        record
recordof(infile);
attrRecord;
string exprText;
integer value3;
        end;


resultsRecord extractResults(infile l, attrRecord attr) := 
        TRANSFORM
            SELF := l;
            SELF := attr;
            SELF.exprText := MATCHTEXT;
            SELF.value3 := MATCHROW(e0[3]).value;
        END;
            

output(PARSE(infile,line,expr,extractResults(LEFT, $1),first,whole,parse,skip(ws)));

