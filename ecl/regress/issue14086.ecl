l2n(string1 letter) := case(letter,
                            'A' => 0,       'K' => 10,       'U' => 20,
                            'B' => 1,       'L' => 11,       'V' => 21,
                            'C' => 2,       'M' => 12,       'W' => 22,
                            'D' => 3,       'N' => 13,       'X' => 23,
                            'E' => 4,       'O' => 14,       'Y' => 24,
                            'F' => 5,       'P' => 15,       'Z' => 25,
                            'G' => 6,       'Q' => 16,
                            'H' => 7,       'R' => 17,
                            'I' => 8,       'S' => 18,
                            'J' => 9,       'T' => 19,
                            error('Valid letter not provided: [' + letter + ']'));
                            // -1);       // works if not using error

sampleExternals :=
  dataset([
    {'B000'}
  ],{string4 db_ext});

// ds0 := sampleExternals;     // works if read inline
ds0 := dataset('~jira::sample',{string4 db_ext},thor);

ds := project(ds0,
              transform({string4 orig, unsigned4 db_int},
                        self.orig := left.db_ext,
                        self.db_int := l2n(left.db_ext[1]),
                        ));

sequential (
  output (sampleExternals,,'~jira::sample',overwrite),
  output(ds,all)
);
