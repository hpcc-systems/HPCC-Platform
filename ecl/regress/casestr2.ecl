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

Default_Data_Location := '~';  // Production value ~

Prefix1(string serviceName) := function
    v1 := if (servicename='x',Default_Data_Location,
          if (servicename='y','~thor400::',
                              Default_Data_Location));

    RETURN v1;
end;

Prefix2(string serviceName) := function
    v2 := case (servicename, 'x' => Default_Data_Location,
                             'y' => '~thor400::',
                                    Default_Data_Location);

    RETURN v2;
end;
Prefix3(string serviceName) := function
    v3 := map (servicename = 'x' => Default_Data_Location,
               servicename = 'y' => '~thor400::',
                                    Default_Data_Location);
    RETURN v3;
end;

output(Prefix1('person_x1') + 'key::mykey');
output(Prefix2('person_x2') + 'key::mykey');
output(Prefix3('person_x3') + 'key::mykey');
