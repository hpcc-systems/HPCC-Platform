#include "EsdlExampleService.hpp"
#include "jliball.hpp"

int main(int argc, char** argv)
{
    InitModuleObjects();
    StringBuffer result;
    onEsdlExampleCppEchoPersonInfo("", "<CppEchoPersonInfoRequest><Name><First>Joe</First><Last>Doe</Last><Aliases><Alias>JD</Alias></Aliases></Name><Addresses><Address><Line1>6601 Park of Commerce Blvd</Line1><City>Boca Raton</City><State>FL</State><Zip>33487</Zip></Address></Addresses></CppEchoPersonInfoRequest>", result);
    DBGLOG("%s", result.str());
    return 0;
}
