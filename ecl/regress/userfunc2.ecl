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




doSomething(boolean isFCRA, integer x, integer y) := define function

    unknown := IF(isFCRA, 0, 99) : stored('unknown');


   return x+y*unknown;
end;

one := 1 : stored('one');
two := 2 : stored('two');


output(doSomething(true, one, two));
