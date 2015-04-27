
boolean MyFunc() := BEGINC++

extern bool doesNotExist();
#body
    return doesNotExist();

ENDC++;

output(MyFunc());

