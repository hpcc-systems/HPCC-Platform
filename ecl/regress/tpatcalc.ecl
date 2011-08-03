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

/*

This example demonstrates the use of productions inside parse statements.  Currently only supported in the tomita version of the parse statement.

It includes a use of the MATCHROW() operator to extract the attributes from a particular matched entry.  No really recomended, but it's there if you need it.

*/


pattern ws := [' ','\t'];
token number := pattern('[0-9]+');
token plus := '+';
token minus := '-';

strRec := { string text; };

attrRecord :=
        record
integer     value;
dataset(strRec) txt;
        end;

makeRow(string x) := dataset(row(transform(strRec, self.text := x)));

rule(attrRecord) e0
        // following option expression is werid, and just to test generation of implicit transforms.
        // argument is optional => a default clear is created
        // transform is omitted, but a single input symbol has a compatible record type so implicit $$=$2 created
        := '(' use(attrRecord,expr)? ')'            //implicit  transform($2)
        |   number                                  transform(attrRecord, self.value := (integer)$1; self.txt := makeRow(matchtext($1)))
        |   '-' SELF                                transform(attrRecord, self.value := -$2.value; self.txt := $2.txt + makeRow(matchtext($2)))
        ;

rule(attrRecord) e1
        := e0
        |  SELF '*' e0                              transform(attrRecord, self.value := $1.value * $3.value; self.txt := $1.txt + $3.txt + makeRow(matchtext($1)))
        |  use(attrRecord, e1) '/' e0               transform(attrRecord, self.value := $1.value / $3.value;  self.txt := $1.txt + $3.txt + makeRow(matchtext($1)))
        ;

rule(attrRecord) e2
        := e1
        |  self plus e1                             transform(attrRecord, self.value := $1.value + $3.value;  self.txt := $1.txt + $3.txt + makeRow(matchtext($1)))
        |  self minus e1                            transform(attrRecord, self.value := $1.value - $3.value;  self.txt := $1.txt + $3.txt + makeRow(matchtext($1)))
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

