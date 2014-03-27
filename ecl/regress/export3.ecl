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



NamesRecord := record
string10 first;
string20 last;
        end;
r := RECORD
  unsigned integer4 dg_parentid;
  string10 dg_firstname;
  string dg_lastname;
  unsigned integer1 dg_prange;
  IFBLOCK(SELF.dg_prange % 2 = 0)
   string20 extrafield;
  END;
  NamesRecord nm;
  dataset(NamesRecord) names;
 END;

ds := dataset('ds', r, thor);


//Walk a record and do some processing on it.
#DECLARE(out)
#EXPORT(out, r);
LOADXML(%'out'%, 'FileStructure');

#FOR (FileStructure)
 #FOR (Field)
  #IF (%'{@isEnd}'% <> '')
output('END');
  #ELSE
output(%'@type'%
   #IF (%'@size'% <> '-15' AND %'@isRecord'%='' AND %'@isDataset'%='')
+ %'@size'%
   #end
 + ' ' + %'@label'% + ';');
  #END
 #END
#END

output('Done');


//This could be greatly simplified as (%'{IsAStringMetaInfo/Field[1]/@type}'%='string')
isAString(inputField) := macro
#EXPORTXML(IsAStringMetaInfo, inputField)
#IF (%'IsAString'%='')
 #DECLARE(IsAString)
#END
#SET(IsAString, false)
#FOR (IsAStringMetaInfo)
 #FOR (Field)
  #IF (%'@type'% = 'string')
   #SET (IsAString, true)
  #END
  #BREAK
 #END
#END
%IsAString%
endmacro;


getFieldName(inputField) := macro
#EXPORTXML(GetFieldNameMetaInfo, inputField)
%'{GetFieldNameMetaInfo/Field[1]/@name}'%
endmacro;


displayIsAString(inputField) := macro
output(getFieldName(inputField) + trim(IF(isAString(inputField), ' is', ' is not')) + ' a string.')
endmacro;

sizeof(r.dg_firstname);
isAString(r.dg_firstname);
getFieldName(r.dg_firstname);
output('ds.dg_firstname isAString? ' + (string)isAString(ds.dg_firstname));
isAString(ds.nm);
sizeof(ds.nm);

displayIsAString(ds.nm);
displayIsAString(r.dg_firstname);
