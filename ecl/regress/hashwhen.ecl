

//#when(legacy)

x := function

   output ('ok!');
   return 3;
END;

x;


//#when(modern)

y := function

   output ('not ok!'); // error
   return 3;
END;

y;
