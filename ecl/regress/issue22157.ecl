PATTERN ws := [' ','\t'];
TOKEN number := PATTERN('[0-9]+');
TOKEN plus := '+';
TOKEN minus := '-';attrRec := RECORD
 REAL val;
END;RULE(attrRec) e0 :=   '(' USE(attrRec,expr)? ')'
                    | number TRANSFORM(attrRec, SELF.val := (INTEGER)$1;)
                    | '-' SELF TRANSFORM(attrRec, SELF.val := -$2.val;);
RULE(attrRec) e1 :=   e0
                    | SELF '*' e0 TRANSFORM(attrRec, SELF.val := $1.val * $3.val;)
                    | USE(attrRec, e1) '/' e0 TRANSFORM(attrRec, SELF.val := $1.val / $3.val;);
RULE(attrRec) e2 :=   e1
                    | SELF plus e1 TRANSFORM(attrRec, SELF.val := $1.val + $3.val;)
                    | SELF minus e1 TRANSFORM(attrRec, SELF.val := $1.val - $3.val;);
RULE(attrRec) expr := e2;infile := DATASET([{'1+2*3'},{'1+2*z'},{'1+2+(3+4)*4/2'},{'2+1/2+1/6+1/24+1/120+1/720+1/5040'}],{ STRING line });resultsRec := RECORD
 DATASET(attrRec) ds;
END;

resultsRec extractResults(infile l, attrRec attr) := TRANSFORM
 DATASET(attrRec) xxx := MATCHROW(e0);  // Syntax failure if included
 SELF.ds  := xxx;
END;
OUTPUT(PARSE(infile,line,expr,extractResults(LEFT, $1), FIRST,WHOLE,PARSE,SKIP(ws)));

