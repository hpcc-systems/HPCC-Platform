fn_test(input,regex) :=FUNCTIONMACRO
      PATTERN p := PATTERN( regex );
      RETURN PARSE(DATASET([{input}],{unicode c}),c,p,{INTEGER4 match_position := MATCHPOSITION(p),UNICODE match_text :=MATCHUNICODE(p)}, MAX ,MANY ,BEST);
ENDMACRO;

content:=u'健康増進法に規定する健康増進事業実施者として、「健康増進事業実施者に対する健康診査の実施等に関する指針」（平成16年厚生労働省告示第242号）や、「健康保険法に基づく保健事業の実施等に関する指針」（平成16年厚生労働省告示第308号）に基づき、被保険者等の健康の保持増進のための健康教育・健康相談・健康診査等の事業を積極的に実施するとともに、専門スタッフを活用した保健指導や健康づくりに取り組むこと。';
output(fn_test(content,u'(健康増進法|健康保険法|保険法)'));

UNICODE names :=u'(健康増進法|健康保険法|保険法)';
output(fn_test(content,names));
