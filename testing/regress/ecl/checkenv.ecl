/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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

pathEnv := GETENV('PATH','unknown');
unknownEnv := GETENV('UNKNOWN_ENV','unknown');

//Normally GETENV is executed at runtime
output(pathEnv != 'unknown');
output(unknownEnv = 'unknown');

// But by using a #declare and #set you can force the expression to be evaluated at compile time
#DECLARE(constPath)
#SET(constPath, pathEnv);
output(%'constPath'% != 'unknown');  // constant folded

#IF (%'constPath'% != 'unknown') // check use in a constant expression
output('good');
#ELSE
output('bad');
#END

#DECLARE(constUnknown)
#SET(constUnknown, unknownEnv);
output(%'constUnknown'% = 'unknown');  // constant folded

#IF (%'constUnknown'% = 'unknown') // check use in a constant expression
output('good');
#ELSE
output('bad');
#END



