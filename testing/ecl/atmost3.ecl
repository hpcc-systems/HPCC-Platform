//This test really should work in thor, but there appear to be issues with the CATCH processing
//nothorlcr

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

j := join(namesTable, addressTable, left.surname = right.surname, TRANSFORM(LEFT), KEEP(1),limit(1),LEFT OUTER);
c := catch(j, ONFAIL(TRANSFORM(namesRecord, SELF.surname := 'Correctly failed', SELF := [])));
output(c);
