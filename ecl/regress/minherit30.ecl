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

//Unusual, but there is some pre-existing code which works along these lines
//An attribute defines a (virtual) module, and that module contains another module.
//Values from the contained module are used as constants within that attribute.

//Strictly speaking the nested module isn't complete until the outer module is complete
//since it might depend on values which are overriden.  However in this case force
//evaluation if it is non-abstract.


IName := interface
    export string name;
end;

f(string prefix) := MODULE(IName)
    SHARED extra := MODULE
        EXPORT STRING addPrefix := 'true';
    END;

    SHARED STRING fullPrefix := IF(#expand(extra.addPrefix), prefix + ' ', '');

    EXPORT name := fullPrefix  + 'Halliday';
END;



OUTPUT(f('Mr').name);
