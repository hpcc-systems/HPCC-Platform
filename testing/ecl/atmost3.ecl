namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

addressRecord :=
            RECORD
string20        surname;
string30        addr := 'Unknown';
unsigned8       age := 0;
            END;

namesTable := dataset([
        {'Smithe','Pru',10},
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smith','Jo'},
        {'Smith','Matthew'},
        {'X','Z'}], namesRecord);

addressTable := dataset([
        {'Hawthorn','10 Slapdash Lane'},
        {'Smith','Leicester'},
        {'Smith','China'},
        {'X','12 The burrows'},
        {'Z','The end of the world'}
        ], addressRecord);

sequential(
output(sort(join(namesTable, addressTable, left.surname = right.surname, TRANSFORM(LEFT), KEEP(1),limit(1),LEFT OUTER),surname,forename))
);
