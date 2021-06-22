IMPORT lib_nlp;
nlp := lib_nlp.nlp;

text01 := 'The quick brown fox jumped over the lazy boy.';
parsedtext01 := nlp.AnalyzeText('parse_en-us',text01);
output(parsedtext01);

text02 := 'TAI has bought the American Medical Records Processing for more than $130 million dollars.';
parsedtext02 := nlp.AnalyzeText('corporate',text02);
output(parsedtext02);

text03 := 'Right middle lobe consolidation compatible with acute pneumonitis.';
parsedtext03 := nlp.AnalyzeText('parse_en-us',text03);
output(parsedtext03);

text04 := 'TAI\'s stock is up 4% from $58.33 a share to $60.66.';
parsedtext04 := nlp.AnalyzeText('corporate',text04);
output(parsedtext04);