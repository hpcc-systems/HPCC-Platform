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

#option ('globalFold', false);
//this returns false

bingo_phonecalls1(string phone) := 
     phone in 
['2012899613',
 '2012899645',
 '498942000000'];


bingo_phonecalls1('2012899613');

//but this returns true

bingo_phonecalls2(string phone) := 
     phone in 
['2012899613',
// '2012899645',    //only change is commenting out this line
 '498942000000'];


bingo_phonecalls2('2012899613');

//and this returns true

bingo_phonecalls3(string10 phone) := //only change from first example is implicit string length
     phone in 
['2012899613',
 '2012899645',  
 '498942000000'];


bingo_phonecalls3('2012899613');
