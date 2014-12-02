
boolean MyFunc() := BEGINC++

    char * temp = "Invalid const string assignment";
    
    char * x = strdup(temp);
    iff (syntaxError);
    
    return false;

ENDC++;

output(MyFunc());

