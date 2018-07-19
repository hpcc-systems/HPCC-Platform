r1 := { STRING10 x };

r := RECORD(r1)
   UNSIGNED filepos{virtual(fileposition)};
END;

dsin := dataset(['One','','Two','','Three','','Four','','Five','','Six','','Seven','','Eight','','Nine','','Ten',''], r1);
ds := DATASET('~regress::one_to_ten', r, thor);
p := ds(x != '') : PERSIST('noblanks');
filtered := p(filepos < 100);

sequential(
    output(dsin,,'~regress::one_to_ten',overwrite);
    output(filtered);
);
