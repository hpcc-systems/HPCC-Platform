// LOADXML within a loop really doesn't work because it tends to replace any local variable definitions
// so the accumulation variables tend to be replaced.
LOADXML('<xml/>');
#DECLARE(n);
#DECLARE(sum);
#SET(sum,1)
#SET(n,1)
#LOOP
   #IF(%n% <= 4)
       LOADXML('<xml><value>' + %n% + '</value></xml>');
       #SET(n,%n% + 1)
       #SET(sum, %sum% + %n%)
   #ELSE
       #BREAK
   #END
#END

output(%value%);
