rec := RECORD
    STRING field1;
    STRING field2;
    INTEGER field3;
    INTEGER field4;
    STRING field5;
END;

ds1 := DATASET([{'0000000000','abcdefghijklmnopqr',159,666,'abcdefghijklmnopq' },
                 {'0000000001','abcdefg',134,217,'abcdefghijklmnopqrstuvwxyz012' },
                 {'0000000002','abcdefghijk',255,779,'abcdefgh' },
                 {'0000000003','abcdefghijklmnop',33,94,'abcdefghijklPQRST' },
                 {'0000000004','abcdefghijklmnopqrstuv',253,519,'abcdefghijkl' }
                 ], rec);
output(ds1, NAMED('Original'));
output(ds1,,'~data-with-exotic-sep_a1.csv', CSV(SEPARATOR('Ý'), HEADING('field1Ýfield2Ýfield3Ýfield4Ýfield5\n')), OVERWRITE);
output(ds1,,'~data-with-exotic-sep_a2.csv', CSV(SEPARATOR('Ý')), OVERWRITE);
output(ds1,,'~data-with-exotic-sep_u1.csv', CSV(UTF8, SEPARATOR('Ý'), HEADING('field1Ýfield2Ýfield3Ýfield4Ýfield5\n')), OVERWRITE);
output(ds1,,'~data-with-exotic-sep_u2.csv', CSV(UTF8, SEPARATOR(U'Ý'), HEADING), OVERWRITE);
output(ds1,,'~data-with-exotic-sep_u3.csv', CSV(UTF8, SEPARATOR([U'Ý',U'-Ý',U'Ý-']), HEADING), OVERWRITE);

ds2 := DATASET('~data-with-exotic-sep.csv', rec, csv( SEPARATOR('Ý')  ) );
output(ds2, NAMED('Created_from_exotic'));
