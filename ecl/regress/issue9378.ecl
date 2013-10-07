namesRecord :=
            RECORD
string20        surname := '?????????????';
string10        forename := '?????????????';
integer2        age := 25;
            END;

addressRecord :=
            RECORD
string30        addr := 'Unknown';
string20        surname;
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
        {'X','14 The crescent'},
        {'Z','The end of the world'}
        ], addressRecord);

dNamesTable := namesTable;
dAddressTable := distribute(addressTable, hash(surname));

JoinedF := join (dNamesTable, dAddressTable,
                LEFT.surname = RIGHT.surname, MANY LOOKUP);

output(JoinedF,,'out.d00');
