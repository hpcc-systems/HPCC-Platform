IMPORT Std.Uni;

EXPORT TestRepeat := MODULE

   EXPORT TestConst := MODULE
    EXPORT Test2 := ASSERT(Uni.Repeat('Repeat this string ', 1) = 'Repeat this string ');

    /*//UNICODE str1 := 'Repeat this string ';

    EXPORT Test1 := ASSERT(Uni.Repeat('Repeat this string ', 0) = '');
    EXPORT Test2 := ASSERT(Uni.Repeat('Repeat this string ', 1) = 'Repeat this string ');
    //EXPORT Test3 := ASSERT(Uni.FindCount(Uni.Repeat('Repeat this string ', 2), 'Repeat this string ') = 2);
    //UNICODE str4 := Uni.Repeat('Repeat this string ', 10);
    //EXPORT Test4 := ASSERT((Uni.FindCount(str4, 'Repeat this string ') = 10) & (length(str4) = length('Repeat this string ')*10) );
    
    //UNICODE str2 := '';
    
    EXPORT Test5 := ASSERT(Uni.Repeat(U'', 0) = '');
    EXPORT Test6 := ASSERT(Uni.Repeat(U'', 1) = '');
    EXPORT Test7 := ASSERT(Uni.Repeat(U'', 2) = '');
    EXPORT Test8 := ASSERT(Uni.Repeat(U'', 10) = '');

    //UNICODE str3 := 'r';
    
    EXPORT Test9 := ASSERT(Uni.Repeat(U'r', 0) = '');
    EXPORT Test10 := ASSERT(Uni.Repeat(U'r', 1) = 'r');
    EXPORT Test11 := ASSERT(Uni.Repeat(U'r', 2) = 'rr');
    EXPORT Test12 := ASSERT(Uni.Repeat(U'r', 10) = 'rrrrrrrrrr');*/
   END;
 
END;