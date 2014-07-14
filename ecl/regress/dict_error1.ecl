

wordRec := { string word; };

inputRec := RECORD
   unsigned id;
   dataset(wordRec) words;
END;


inDs := DATASET([
    {1, [{'cat'},{'dog'},{'cow'},{'chicken'}]},
    {2, [{'fish'},{'hamster'},{'cat'}]},
    {3, [{'tortoise'}]},
    {3, []}], inputRec);

processedRec := RECORD
   unsigned id;
   DICTIONARY(wordRec) words;
END;

processedDict := PROJECT(inDs, TRANSFORM(processedRec, SELF := LEFT)); // Need to convert the dataset to a dictionary

output(processedDict,,THOR);
