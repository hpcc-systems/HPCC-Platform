SomeFile1 := DATASET([{'A'},{'B'},{'C'},{'D'},{'E'},
                     {'F'},{'G'},{'H'},{'I'},{'J'},
                     {'K'},{'L'},{'M'},{'N'},{'O'},
                     {'P'},{'Q'},{'R'},{'S'},{'T'},
                     {'U'},{'V'},{'W'},{'X'},{'Y'}],
                     {STRING1 Letter});

Sample_interval := 5 : stored('x');
Sample_number := 1 : stored('y');
string1 Letter := 'Z' : stored('Letter');

MySet1 := SAMPLE(SomeFile1,Sample_interval,IF(Letter='A',Sample_number*10, 1));

MySet1;


r1 := {unsigned i};
r2 := {STRING1 Letter};
r3 := RECORD(r1)
    DATASET(r2) letters;
END;

ds := DATASET(5, transform({unsigned i}, SELF.i := COUNTER));


r3 t(r1 l) := TRANSFORM
    SELF.letters := CHOOSEN(SAMPLE(SOmeFile1, Sample_interval, l.i), 2);
    SELF := l;
END;

OUTPUT(PROJECT(ds, t(LEFT)));
