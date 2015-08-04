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




textToString(value) := macro

#if (value<=3)
    #if (value=1)
'One'
    #elseif (value=2)
'Two'
    #else
'Three'
    #end
#elseif (value<=10)
'Single digit'
#elseif (value<=100)
    #if (value < 20)
'Teens'
    #elseif (value <50)
'Low Double digit'
    #else
'High Double digit'
    #end
#else
'Very large'
#end

endmacro;

textToString(1);
textToString(2);
textToString(3);
textToString(8);
textToString(13);
textToString(25);
textToString(67);
textToString(1000);
