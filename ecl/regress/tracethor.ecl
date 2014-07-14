

rec := RECORD
    UNSIGNED id;
    STRING text;
END;

ds := DATASET(1000, transform(rec, SELF.id := COUNTER; SELF.text := 'x' + (string)RANDOM()), LOCAL);

p := ds : independent;

s := sort(p, text, local);

output(s);
