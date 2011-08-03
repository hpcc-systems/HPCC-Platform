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

//Code to try and analyse accounts, but in as many strange ways as possible.....
//input is an open office spread seat with 3 columns: Date, Description and amount.

import lib_stringlib;
import lib_unicodelib;

rec :=      RECORD,maxlength(99999)
string          month{xpath('/office:document-content/office:body/table:table/@table:name')};
varstring       date{xpath('table:table-cell[1]/text:p')};
varunicode      description{xpath('table:table-cell[2]/text:p')};
varunicode      fullDescription{xpath('table:table-cell[2]/text:p<>')};
varstring       amount{xpath('table:table-cell[3]/text:p')};
            END;

string8 firstDate := '20040301' : stored('firstDate');
qstring8 lastDate := '20049999' : stored('lastDate');
unicode contains := U'' :stored('contains');

decimal8 numListResults := 10 : persist('numListResults');
decimal8 numSummaryResults := 2000 : persist('numSummaryResults');

accounts := dataset('~file::127.0.0.1::temp::content.xml', rec, XML('/office:document-content/office:body/table:table/table:table-row'));

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
        self.amount := (typeof(self.amount))(stringlib.StringFilterOut(l.amount,','));
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
    self.description := MAP(l.description=U'Cheque Payment' and l.amount in [-24.00D,-20.00D] => U'House Cleaner',
                            l.description=U'Cheque Payment' and l.amount in [-14.00D] => U'Window Cleaner',
                            l.description=U'Cheque Payment' and l.amount in [-152D,-212D,-243D,-114D,-124.5D,-123D,-109D,-452D] => U'Child care',
                            l.description=U'Cheque Payment' and l.amount in [-447.19D] => U'Lawn mower',
                            l.description=U'Cheque Payment' and l.amount in [-33D,-16.5D] => U'Trombone',
                            l.description=U'Cheque Payment' and l.amount in [-325.02D] => U'Car',
                            l.description=U'Cheque Payment'=>U'Unknown '+l.description,
//                          l.description=U'Cheque Payment'=>U'Unknown '+l.description+U' ' +(unicode)l.amount,
                            l.description=U'CHRIS PAGE GAVIN'=>U'CHRIS PAGE',
                            (string)l.description[lengthDescription]='.'=>l.description[1..lengthDescription-1],
                            unicodelib.UnicodeToLowerCase(l.description)[1..16] =U'victory outreach'=>U'Victory Outreach',
                            l.description);
    self := l;
    END;

allProcessed := project(processed2, extractMatched3(left));

sortDedupAll := dedup(sort(allProcessed,-abs(amount)), description);

output(choosen(sortDedupAll,(integer)numListResults));

summarised := table(allProcessed, { total := sum(group, amount), cnt := count(group), description, }, description);
sortSummary := sort(summarised, -abs(total));

output(sortSummary(total>0),,'~summary.in.csv',csv,overwrite);
output(sortSummary(total<0),,'~summary.out.csv',csv,overwrite);

output(allProcessed, { 'Total = '+(string)sum(group, amount), 'Total out = '+(string)sum(group, amount, amount <0), 'Average = ', (string)ave(group, amount, amount<0)});

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
