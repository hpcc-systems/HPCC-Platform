namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

xRecord :=
            RECORD
string20        surname;
string10        forename;
            END;

namesTable := index(namesRecord,'i');


x := DATASET(['Smith','Blogs'], { string20 surname, });

p := PROJECT(NOFOLD(x), transform(xRecord, SELF.forename := 'John'; SELF := LEFT));

j := JOIN(p, namesTable, LEFT.surname = RIGHT.surname AND LEFT.forename = 'John', ATMOST(100), LOOKUP, MANY);

output(j,,'out.d00',overwrite);
