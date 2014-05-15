ds := dataset('x', { unsigned id; }, thor);
output(ds,XMLNS('bob1','OhlàlàStraße'));
output(ds,XMLNS('bob2',U8'OhlàlàStraße'));
output(ds,XMLNS('bob3',(ebcdic string)'OhlàlàStraße'));     // You really have to be daft to use an ebcdic string...
