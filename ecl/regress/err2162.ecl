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

aaa := DATASET('aaa', {STRING1 fa}, hole);
bbb := DATASET('bbb', {STRING1 fb}, hole, aaa);
OUTPUT(GROUP(aaa, GROUP(bbb, fb)[1].fb));


/*
aaa := DATASET('aaa', {STRING1 fa, STRING1 fb}, hole);
bbb := DATASET('aaa', {STRING1 fa, STRING1 fb}, hole, aaa);

NestedGroup := Group(aaa, Group(bbb,bbb.fa)[1].fa);

GroupedSet := Group(aaa, aaa.fa);

SecondSort := SORT(GroupedSet, aaa.fb);

// This gives: an Unhandled operator in SqlSelect::gatherSelects()
Output(SecondSort);

MyTable := TABLE(SecondSort, {aaa.fa, aaa.fb});

// This gives: assert false in hqlexpr.cpp
OUTPUT(Mytable);
*/
