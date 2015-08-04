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

//good
VariableString(integer len) := TYPE
  export integer   physicalLength(string physical) := (integer)len;
  export string    load(string physical) := physical;
  export string    store(string logical) := logical;
END;

VariableStringx(integer len) := TYPE
  export integer   physicalLength := 10;
  export string    load(string physical) := physical;
  export string    store(string logical) := logical;
END;

// bad
VariableString1(integer len) := TYPE
//  export integer   physicalLength(string physical) := (integer)len;
  export string    load(string physical) := physical;
  export string    store(string logical) := logical;
END;


VariableString2(integer len) := TYPE
  export integer   physicalLength(string physical) := (integer)len;
//  export string      load(string physical) := physical;
  export string    store(string logical) := logical;
END;

VariableString3(integer len) := TYPE
  export integer   physicalLength(string physical) := (integer)len;
  export string    load(string physical) := physical;
//  export string      store(string logical) := logical;
END;

VariableString4(integer len) := TYPE
  export integer   physicalLength(string2 physical) := (integer)len;
  export string    load(string2 physical) := physical;
  export string    store(string logical) := logical;
END;

VariableString5(integer len) := TYPE
  export string    physicalLength(string physical) := 'abc';
  export string    load(string physical) := physical;
  export string    store(string logical) := logical;
END;
VariableString6(integer len) := TYPE
  export String    physicalLength := 'abc';
  export string    load(string physical) := physical;
  export string    store(string logical) := logical;
END;

// value macro is OK, but need to be valid
VariableString6xx(integer len) := TYPE
  export physicalLength := MACRO 3x ENDMACRO;
  export string    load(string physical) := physical;
  export string    store(string logical) := logical;
END;

VariableString6x(integer len) := TYPE
  export physicalLength := MACRO a:=3; ENDMACRO;
  export string    load(string physical) := physical;
  export string    store(string logical) := logical;
END;

VariableString6y(integer len) := TYPE
  export  physicalLength := DATASET('aaa',{STRING1 a;},FLAT);
  export string    load(string physical) := physical;
  export string    store(string logical) := logical;
END;


VariableString7(integer len) := TYPE
  export integer   physicalLength(string5 physical) := (integer)len;
  export integer   load(string5 physical) := (integer)physical;
  export string5   store(integer a) := (String5)a;
END;
