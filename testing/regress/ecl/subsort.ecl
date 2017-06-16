d := dataset([
 { 'A', 1 },
 { 'A', 4 },
 { 'A', 2 },
 { 'A', 3 },
 { 'B', 1 },
 { 'B', 3 },
 { 'B', 4 },
 { 'B', 2 },
 { 'B', 5 },
 { 'A', 5 }
], { string a, integer b });

SUBSORT(d, {a}, {b});


//Test implicit subsort where some of the sort fields are duplicated with cast versions of the same fields.

{unsigned2 a,unsigned2 b,unsigned2 c,unsigned2 d,unsigned2 e} t(unsigned cnt) := TRANSFORM
    SELF.a := cnt;
    SELF.b := cnt % 10;
    SELF.c := HASH64(cnt);
    SELF.d := HASH32(cnt) % 10;
    SELF.e := HASH(cnt) % 10;
END;

ds := dataset(200, t(COUNTER));

s1 := SORT(ds, (unsigned8)d,(unsigned8)e,RECORD);
f1 := NOCOMBINE(s1);

s2 := SORT(f1, (unsigned8)d,(unsigned8)e,b,RECORD);
f2 := NOCOMBINE(s2);
s3 := SORTED(f2, (unsigned8)d,(unsigned8)e,b,RECORD, ASSERT,LOCAL);
output(s3);
