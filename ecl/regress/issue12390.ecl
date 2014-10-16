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
