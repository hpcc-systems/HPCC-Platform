

// CSocketSelectThread error 10038  

#include <jmisc.hpp>
#include <mpbase.hpp>
#include <mpcomm.hpp>

using namespace std;

#define MULTITEST

int main(int argc, char* argv[]){
    InitModuleObjects();
    try {
        EnableSEHtoExceptionMapping();

        startMPServer(0);
        IGroup* group = createIGroup(0, (INode **) NULL);
        ICommunicator* comm = createCommunicator(group);
        PrintLog("Hello from %d. Total of %d nodes.",group->rank(), group->ordinality());    
        stopMPServer();
    } catch (IException *e){
        pexception("Exception", e);
    }
    return 0;
}
