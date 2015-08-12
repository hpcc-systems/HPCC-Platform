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




outputRow(showA = false, showB = false, showC = false, aValue = '\'abc\'', bValue = 10, cValue = true) := macro
    output(IF(showA,' a='+aValue,'')+if(showB,' b='+(string)bValue,'')+if(showc,' c='+(string)cValue,''))
    endmacro;

outputRow();
outputRow(true);
outputRow(,,true);

outputRow(named    showB := true);
outputRow(true, named aValue := 'Changed value');
outputRow(,,,'Changed value2',named showA := true);

outputRow(showB := true);
outputRow(true, aValue := 'Changed value');
outputRow(,,,'Changed value2',showA := true);

boolean showA := false;
string showB := '';

outputRow(showB := true);
outputRow(true, aValue := 'Changed value',showB:=true);
outputRow(,,,'Changed value2',showA := true);
