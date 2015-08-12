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

test( arg1, arg2='[]' ) := MACRO
    output('in macro test. arg1 = ' + arg1 );
    #uniquename(c)
    %c% := (string)count(arg2);
    output( 'You entered ' + %c% + ' items in arg2');
ENDMACRO;

// "Too many actual parameters supplied to macro test: expected 2, given 3"
test('a', ['spam','eggs'] );

// no problem:
test('b', ['spam'] );

// no problem:
setArg := ['spam','eggs'];
test('c', setArg );


