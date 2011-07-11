/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
