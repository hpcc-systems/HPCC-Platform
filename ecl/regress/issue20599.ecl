DA := DATASET([{'a',1}
              ,{'a',2}
              ,{'b',3}
              ,{'c',4}
              ,{'c',5}
              ,{'c',6}
              ],{STRING1 id,UNSIGNED val});
DB := DATASET([{'a','a'}
              ,{'a','a'}
              ,{'b','a'}
              ,{'c','d'}
              ,{'c','d'}
              ,{'c','e'}
              ],{STRING1 id,STRING1 other});

ROUT := RECORD
   UNSIGNED Result;
   UNSIGNED AnotherResult;
END;

ROUT FailsSyntaxCheck(RECORDOF(DA) L,RECORDOF(DB) R) := TRANSFORM

    SELF := MAP(L.val = 1   => MAP(R.id = 'a'   => ROW({0,0},ROUT)
                                  ,R.id = 'b'   => ROW({1,1},ROUT)
                                  ,                ROW({2,3},ROUT)
                                  )
               ,L.val = 2   => ROW({4,4},ROUT)
               ,               ROW({5,5},ROUT)
                               );
END;

ROUT PassesSyntaxCheck(RECORDOF(DA) L,RECORDOF(DB) R) := TRANSFORM

     Innr := MAP(R.id = 'a' => ROW({0,0},ROUT)
                ,R.id = 'b' => ROW({1,1},ROUT)
                ,              ROW({2,3},ROUT)
                );
     SELF := MAP(L.val = 1   => Innr
                ,L.val = 2   => ROW({4,4},ROUT)
                ,               ROW({5,5},ROUT)
                );
END;
JOIN(DA,DB,LEFT.id = RIGHT.id,FailsSyntaxCheck(LEFT,RIGHT));
JOIN(DA,DB,LEFT.id = RIGHT.id,PassesSyntaxCheck(LEFT,RIGHT));
