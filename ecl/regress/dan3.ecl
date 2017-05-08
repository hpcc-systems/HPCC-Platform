SomeFunc() := FUNCTIONMACRO

/*
    STRING MarkNodes(STRING l, STRING l_nodes, STRING n, STRING n_nodes) := BEGINC++
        #option pure
        #body
        unsigned long i = 0;
        char * out = (char*)rtlMalloc(lenN_nodes);
        memcpy(out, n_nodes, lenN_nodes);
        while ( i < lenN){
                if (l[i] != n[i] || i >= lenL){
                    out[i] = '1';
                    break;
                }
                if (l_nodes[i] != n_nodes[i]){
                    out[i] = '1';
                }
                i += 1;
        }
        __lenResult = lenN_nodes;
        __result = out;
    ENDC++;
*/

    STRING MarkNodes(STRING l, STRING l_nodes, STRING n, STRING n_nodes) := EMBED(C++ : DISTRIBUTED)
        #option pure
        #body
        unsigned long i = 0;
        char * out = (char*)rtlMalloc(lenN_nodes);
        memcpy(out, n_nodes, lenN_nodes);
        while ( i < lenN){
                if (l[i] != n[i] || i >= lenL){
                    out[i] = '1';
                    break;
                }
                if (l_nodes[i] != n_nodes[i]){
                    out[i] = '1';
                }
                i += 1;
        }
        __lenResult = lenN_nodes;
        __result = out;
    ENDEMBED;


    RETURN MarkNodes('v1', 'v2', 'v3', 'v4');
ENDMACRO;

SomeFunc();
