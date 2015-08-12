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


namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

names := dataset([{'zqlliday','Gavin',10}],namesRecord);


myNames := [ names.surname[1..1], names.surname[2..2], names.surname[3..3], names.surname[4..4] ];


output(names,{
        rank(1, [100,400,200,300]),1,
        rank(2, [100,400,200,300]),4,
        ranked(2, [100,400,200,300]),3,
        ranked(3, [100,400,200,300]),4,
        rank(1, ['a','d','b','c']),1,
        rank(2, ['a','d','b','c']),4,
        ranked(2, ['a','d','b','c']),3,
        ranked(3, ['a','d','b','c']),4,
        rank(1, ['aaa','aad','aab','aac']),1,
        rank(2, ['aaa','aad','aab','aac']),4,
        ranked(2, ['aaa','aad','aab','aac']),3,
        ranked(3, ['aaa','aad','aab','aac']),4,
        rank(1, [age+100,age+400,age+200,age+300]),1,
        rank(2, [age+100,age+400,age+200,age+300]),4,
        ranked(2, [age+100,age+400,age+200,age+300]),3,
        ranked(3, [age+100,age+400,age+200,age+300]),4,
        rank(1, [surname+'a',surname+'d',surname+'b',surname+'c']),1,
        rank(2, [surname+'a',surname+'d',surname+'b',surname+'c']),4,
        ranked(2, [surname+'a',surname+'d',surname+'b',surname+'c']),3,
        ranked(3, [surname+'a',surname+'d',surname+'b',surname+'c']),4,
        rank(1, [surname+'aaa',surname+'aad',surname+'aab',surname+'aac']),1,
        rank(2, [surname+'aaa',surname+'aad',surname+'aab',surname+'aac']),4,
        ranked(2, [surname+'aaa',surname+'aad',surname+'aab',surname+'aac']),3,
        ranked(3, [surname+'aaa',surname+'aad',surname+'aab',surname+'aac']),4,
        rank(1, [100,400,200,300],desc),4,
        rank(2, [100,400,200,300],desc),1,
        ranked(2, ['aaa','aad','aab','aac'],desc),4,
        ranked(3, ['aaa','aad','aab','aac'],desc),3,
        ranked(2, [surname[1..1+2],surname[2..2+2],surname[3..3+2],surname[4..4+2]],desc),-1,
        rank(2, [surname[1..1+2],surname[2..2+2],surname[3..3+2],surname[4..4+2]],desc),-1,
        '------------',
        RANKED(1,myNames),
        RANK(2,[10,40,30,20]),
        RANK(2,[20,30,10,100]),
        RANK(1,[20,30,10,100]),
        RANKED(1,[20,30,10,100]),
        RANK(1,[20,30,10,100],descend),
        RANKED(1,[20,30,10,100],desc),
        'abc'[1..2],
        'abc'[2..],
        'abc'[..2],
        'abc'[2],
        '$',
        (string2)('abc'[1+0..2+0]),
        (string2)('abc'[2+0..]),
        (string2)('abc'[..2+0]),
        'abc'[2+0],
        },'out.d00');


