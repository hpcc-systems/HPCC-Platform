/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

//class=embedded
//class=embedded-js
//class=3rdparty

//nothor

IMPORT javascript;

integer add1(integer val) := EMBED(javascript:FOLD)
val+1
ENDEMBED;

string add2(string val) := EMBED(javascript:FOLD)
val+'1'
ENDEMBED;

string add3(varstring val) := EMBED(javascript:FOLD)
val+'1'
ENDEMBED;

utf8 add4(utf8 val) := EMBED(javascript:FOLD)
val+'1'
ENDEMBED;

unicode add5(unicode val) := EMBED(javascript:FOLD)
val+'1'
ENDEMBED;

utf8 add6(utf8 val) := EMBED(javascript:FOLD)
val+'1'
ENDEMBED;

unicode add7(unicode val) := EMBED(javascript:FOLD)
val+'1'
ENDEMBED;

data testData(data val) := EMBED(javascript:FOLD)
val[0] = val[0] + 1;
val;
ENDEMBED;

set of integer testSet(set of integer val) := EMBED(javascript:FOLD)
val.slice(0,2);
ENDEMBED;

set of string testSet2(set of string val) := EMBED(javascript:FOLD)
val.slice(0,2);
ENDEMBED;

set of string testSet3(set of string8 val) := EMBED(javascript:FOLD)
val.slice(0,2);
ENDEMBED;

set of utf8 testSet4(set of utf8 val) := EMBED(javascript:FOLD)
val.slice(0,2);
ENDEMBED;

set of varstring testSet5(set of varstring val) := EMBED(javascript:FOLD)
val.slice(0,2);
ENDEMBED;

set of varstring8 testSet6(set of varstring8 val) := EMBED(javascript:FOLD)
val.slice(0,2);
ENDEMBED;

set of unicode testSet7(set of unicode val) := EMBED(javascript:FOLD)
val.slice(0,2);
ENDEMBED;

set of unicode8 testSet8(set of unicode8 val) := EMBED(javascript:FOLD)
val.slice(0,2);
ENDEMBED;

ASSERT(add1(10)=11, CONST);
ASSERT(add2('Hello')='Hello1', CONST);
ASSERT(add3('World')='World1', CONST);
ASSERT(add4(U'Oh là là Straße')=U'Oh là là Straße1', CONST);
ASSERT(add5(U'Стоял')=U'Стоял1', CONST);
ASSERT(add6(U'Oh là là Straße')=U'Oh là là Straße1', CONST);
ASSERT(add7(U'Стоял')=U'Стоял1', CONST);

ASSERT(add2('Oh là là Straße')='Oh là là Straße1', CONST);  // Passing latin chars - should be untranslated

ASSERT(testData(D'ax')=D'bx', CONST);
ASSERT(testSet([1,3,2])=[1,3], CONST);
ASSERT(testSet2(['red','green','yellow'])=['red','green'],CONST);
ASSERT(testSet3(['one','two','three'])=['one','two'],CONST);

ASSERT(testSet4([U'Oh', U'là', U'Straße'])=[U'Oh', U'là'], CONST);
ASSERT(testSet5(['Un','Deux','Trois'])=['Un','Deux'], CONST);
ASSERT(testSet6(['Uno','Dos','Tre'])=['Uno','Dos'], CONST);

ASSERT(testSet7([U'On', U'der', U'Straße'])=[U'On', U'der'], CONST);
ASSERT(testSet8([U'Aus', U'zum', U'Straße'])=[U'Aus', U'zum'], CONST);

OUTPUT('ok');
