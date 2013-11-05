
d := nofold(DATASET(
  [{1, [{'Richard' => 50}]},
   {1, [{'Richard' => 100}]},
   {1, [{'R2ichard' => 100}]},
   {1, [{'Ri3chard' => 100}]},
   {1, [{'Ric4hard' => 100}]},
   {1, [{'Rich5ard' => 100}]},
   {1, [{'Richa6rd' => 100}]},
   {1, [{'Richar7d' => 100}]},
   {1, [{'Richar8d' => 100}]},
   {1, [{'Richar9d' => 100}]},
   {1, [{'Richard' => 100}]},
   {1, [{'Richard' => 100}]}
  ], { unsigned top, DICTIONARY({STRING8 name => UNSIGNED iq}) nest}));

d t(d L, d R) := TRANSFORM
  SELF.nest := L.nest + R.nest;
  SELF := R;
END;

rollup(d, LEFT.top=RIGHT.top, t(LEFT, RIGHT));
