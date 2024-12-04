OUTPUT(DATASET([{1}], {UNSIGNED1 a}), ,'data');

d := DATASET('data', {UNSIGNED1 a}, TYPE(FLAT));
OUTPUT(d);