GetXLen(DATA x,UNSIGNED len) := TRANSFER(((DATA4)(x[1..len])),UNSIGNED4);
xstring(UNSIGNED len) := TYPE
  EXPORT INTEGER PHYSICALLENGTH(DATA x) := GetXLen(x,len) + len;
  EXPORT STRING LOAD(DATA x) := (STRING)x[(len+1)..GetXLen(x,len) + len];
  EXPORT DATA STORE(STRING x):= TRANSFER(LENGTH(x),DATA4)[1..len] + (DATA)x;
END;

pstr    := xstring(1);
pppstr  := xstring(3);
nameStr := STRING20;
namesRecord := RECORD
  pstr    surname;
  nameStr forename;
  pppStr  addr;
END;

ds := DATASET([{'TAYLOR','RICHARD','123 MAIN'},
               {'HALLIDAY','GAVIN','456 HIGH ST'}],
               {nameStr sur,nameStr fore, nameStr addr});

namesRecord  MoveData(ds L) := TRANSFORM
  SELF.surname  := L.sur;
  SELF.forename := L.fore;
  SELF.addr     := L.addr;
END;
out := PROJECT(ds,MoveData(LEFT));
OUTPUT(out);
