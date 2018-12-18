IMPORT R;

integer testError(integer val) := EMBED(R)
  print val
ENDEMBED;

testError(10)
