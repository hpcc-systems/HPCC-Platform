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


