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

MyRec := RECORD
    STRING1 Value1;
    STRING1 Value2;
END;
SomeFile  := DATASET([{'C','G'},
                      {'C','C'}],MyRec);
OtherFile := DATASET([{'A','X'},
                      {'B','G'},
                      {'A','B'}],MyRec);

MAC_AddCat(AttrName,FirstArg,SecondArg) := MACRO
    AttrName := FirstArg + SecondArg;
ENDMACRO;

MAC_AddCat(AddValues,5,10)
MAC_AddCat(CatValues,'5','10')
MAC_AddCat(JoinFiles,SORT(SomeFile,Value1,Value2),SORT(OtherFile,Value1,Value2))

AddValues;          //result = 15
CatValues;          //result = '510'
output(sort(joinfiles,Value1,Value2));
/* joinfiles results in:
    Rec#    Value1  Value2
    1       A       B
    2       A       X
    3       B       G
    4       C       C
    5       C       G
*/
