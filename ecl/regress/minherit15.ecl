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

//Inheritance from a non virtual base....

baseModule := module
export string50 firstName := 'Gavin';
export string50 lastName := 'Hawthorn';
        end;

string f1(baseModule ctx) := '[' + ctx.firstName + ',' + ctx.lastName + ']';

derivedModule := module(baseModule)
export addr := '10 Rue de Jambon, Paris';
    end;


output(f1(derivedModule));
