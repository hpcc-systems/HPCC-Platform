// ds := dataset([{'the quick brown fox jumped over the lazy red dog, quickly'}],{string line});
// ds := dataset([{'help me'}],{string line});

// x := pipe(ds,'wc --help',{string305  back});

// output(x);

// ds := dataset([
                // {'1 the quick brown fox jumped over the lazy red dog, quickly'},
                // {'2 the quick fox jumped over the lazy red dog'},
                // {'3 the quick brown fox jumped over the lazy red dog'},
                // {'4 the quick brown fox jumped over the lazy red dog, quickly'},
                // {'5 the quick fox jumped over the lazy red dog'},
                // {'6 the quick brown fox jumped over the lazy red dog'},
                // {'7 the quick brown fox jumped over the lazy red dog, quickly'},
                // {'8 the quick fox jumped over the lazy red dog'},
                // {'9 the quick brown fox jumped over the lazy red dog'},
                // {'10 the quick brown fox jumped over the lazy red dog, quickly'},
                // {'11 the quick fox jumped over the lazy red dog'},
                // {'12 the quick brown fox jumped over the lazy red dog'},
                // {'13 the quick brown fox jumped over the lazy red dog, quickly'},
                // {'14 the quick fox jumped over the lazy red dog'},
                // {'15 the quick brown fox jumped over the lazy red dog'},
                // {'16 the quick brown fox jumped over the lazy red dog, quickly'},
                // {'17 the quick fox jumped over the lazy red dog'},
                // {'18 the quick brown fox jumped over the lazy red dog'},
                // {'19 the quick brown fox jumped over the lazy red dog, quickly'},
                // {'20 the quick fox jumped over the lazy red dog'},
                // {'21 the quick brown fox jumped over the lazy red dog'}
               // ],{string line});

// d := distribute(ds,random());

// output(d,,'~RTTEST::OUT::PipeTest');

ds := DATASET('~RTTEST::OUT::PipeTest',{STRING line},THOR);
x := PIPE(ds,'wc',{STRING24 back}); // wc is a Linux word count utility

OUTPUT(x);