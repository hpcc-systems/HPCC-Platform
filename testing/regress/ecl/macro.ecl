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
