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

//class=file
//version multiPart=true

#onwarning(10138, ignore);

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
useLocal := #IFDEFINED(root.useLocal, false);
useTranslation := #IFDEFINED(root.useTranslation, false);

//--- end of version configuration ---

import $.setup;
Files := setup.Files(multiPart, useLocal, useTranslation);

//Code to try and analyse accounts, but in as many strange ways as possible.....
//input is an open office spreadsheet with 3 columns: Date, Description and amount.

import Std.Str;
IMPORT * FROM lib_unicodelib;

rec :=      RECORD,maxlength(99999)
string          month{xpath('/office:document-content/office:body/table:table/@table:name')};
varstring       date{xpath('table:table-cell[1]/text:p')};
varunicode      description{xpath('table:table-cell[2]/text:p')};
varunicode      fullDescription{xpath('table:table-cell[2]/text:p<>')};
varstring       amount{xpath('table:table-cell[3]/text:p')};
            END;

string8 firstDate := '20020101' : stored('firstDate');
qstring8 lastDate := '99999999' : stored('lastDate');
unicode contains := U'' :stored('contains');

decimal8 numListResults := 5 : stored('numListResults');
decimal8 numSummaryResults := 30 : stored('numSummaryResults');

accounts := dataset(Files.DG_FileOut+'accountxml', rec, XML('/office:document-content/office:body/table:table/table:table-row'));

validAccounts := accounts(month != 'Cashflow' and month != 'Spending');

newrec :=       RECORD,maxlength(99999)
string              month;
varstring           date;
varunicode          desc1;
varunicode          description;
unicode             fullDescription;
decimal8_2          amount;
                END;

newrec t(rec l) := transform,skip((l.date='' or l.date='Date') or l.description in [U'Description',U''])
        self.date := l.date[7..10]+l.date[4..5]+l.date[1..2];
        self.desc1 := l.description[1..2] + l.description[length(l.description)];
        self.amount := (typeof(self.amount))(Str.FilterOut(l.amount,','));
        self := l;
        END;

normalized := project(validAccounts, t(left));

filtered := normalized(date >= firstDate and date <= lastDate, contains=U'' or unicodeLib.UnicodeFind(description,contains,1) != 0);

pattern word := nocase(pattern('[a-z]+'));

pattern isCash := U'CASH' ANY*;
pattern isChildBenefit := first repeat(pattern('[0-9]'),12) '-CHB' last;
pattern isPaidIn := first nocase('paid in at');
pattern chequePayment := first ('1' or '2') repeat(['0','1','2','3','4','5','6','7','8','9'], 5) last;
pattern groceries := ('TESCO' not before ' GARAGE') or 'SAINSBURYS' or first 'ASDA ';
pattern petrol := 'TESCO GARAGE' or ('TEXACO' not before (ANY * 'TEAM FLI')) or 'BP WHITLESFORD' or 'FLINT CROSS';
pattern heatingOil := ('TEX' before (ANY * 'TEAM FLI'));
pattern train := first (VALIDATE(pattern('[A-Z]+ [A-Z]+'), MATCHTEXT='WAGN RAILWAY') or 'MIDLAND MAIN LINE');
pattern boots := first 'BOOTS /' pattern('[0-9]{4}') '/' ANY + last;
pattern mobilePhone := first validate(word, MATCHUNICODE = U'ORANGE');
pattern lifeInsurance := (first 'SCOTTISH PROVIDENT' last) or (first U'CGNU LIFE ASS LTD' last);
pattern grammar := isCash or isChildBenefit or isPaidIn or chequePayment or groceries or petrol or heatingOil or train or 
                   boots or mobilePhone or lifeInsurance;


newrec  extractMatched(newrec l) := transform
    self.description := MAP(MATCHED(isCash)=>U'CASH',
                            MATCHED(isChildBenefit)=>U'Child Benefit',
                            MATCHED(isPaidIn)=>U'Cheques Paid In',
                            MATCHED(chequePayment)=>U'Cheque Payment',
                            MATCHED(groceries)=>U'Groceries',
                            MATCHED(petrol)=>U'Petrol',
                            MATCHED(heatingOil)=>U'Heating oil',
                            MATCHED(train)=>U'Train',
                            MATCHED(boots)=>U'Boots Chemist',
                            MATCHED(mobilePhone)=>U'Mobile Phone',
                            MATCHED(lifeInsurance)=>U'Life insurance',
                            l.description);
    self := l;
    END;

processed := parse(filtered, (string)description, grammar, extractMatched(left), not matched, first, noscan);


pattern paymentTarget := any+;
pattern firstPayment := '<text:p>' paymentTarget U' <text:s' any * nocase('First Payment');
pattern grammar2 := firstPayment;


newrec  extractMatched2(newrec l) := transform
    self.description := MAP(MATCHED(firstPayment) => MATCHUNICODE(paymentTarget),
                            l.description);
    self := l;
    END;

processed2 := parse(processed, fullDescription, grammar2, extractMatched2(left), not matched, first, noscan);

newrec  extractMatched3(newrec l) := transform
    unsigned4 lengthDescription := length(l.description);
    self.description := MAP(l.description=U'Cheque Payment' and l.amount in [24.00D,20.00D] => U'House Cleaner',
                            l.description=U'Cheque Payment' and l.amount in [14.00D] => U'Window Cleaner',
                            l.description=U'Cheque Payment'=>U'Unknown '+l.description,
                            l.description=U'CHRIS PAGE JIM AND LUCY '=>U'CHRIS PAGE',
                            (string)l.description[lengthDescription]='.'=>l.description[1..lengthDescription-1],
                            unicodelib.UnicodeToLowerCase(l.description)[1..16] =U'victory outreach'=>U'Victory Outreach',
                            l.description);
    self := l;
    END;

allProcessed := project(processed2, extractMatched3(left));

sortDedupAll := dedup(sort(allProcessed,-abs(amount), description), description);

output(choosen(sortDedupAll,(integer)numListResults));

summarised := table(allProcessed, { total := sum(group, amount), cnt := count(group), description, }, description);
sortSummary := sort(summarised, -abs(total), description);
output(choosen(sortSummary,(unsigned)numSummaryResults));

output(allProcessed, { 'Total = '+(string)sum(group, amount), 'Total out = '+(string)sum(group, amount, amount <0), 'Average = ', (string)round(ave(group, amount, amount<0),3)});

unMapped := allProcessed(not regexfind('[a-z]', (string)description));
//output(unMapped);

candiateRec := record
string  leftDesc;
varstring   rightDesc;
        end;

candiateRec getCandidate(unMapped l, unMapped r) := transform
    self.leftDesc := '"' + (string)l.description + '"';
    self.rightDesc := V'"' + (varstring)r.description + V'"';
    end;

possibleMatches := join(unMapped, unMapped,
                        LEFT.description != RIGHT.description AND
                        unicodeLib.UnicodeFind(LEFT.description, RIGHT.description, 1) != 0, getCandidate(left, right), ALL);
output(possibleMatches);
