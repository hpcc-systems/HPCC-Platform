#pragma warning(disable : 4786)

#ifdef WIN32
#define $$ESP_ABBREV$$_API _declspec (dllexport)
#else
#define $$ESP_ABBREV$$_API
#endif

//Jlib
#include "jliball.hpp"

//SCM Interfaces
#include "$$ESP_SERVICE$$.hpp"

//ESP Generated files
#include "$$ESP_SERVICE$$_esp.ipp"

$$ESP_ABBREV$$_API IClient$$ESP_SERVICE$$ * create$$ESP_SERVICE$$Client()
{
    return new CClient$$ESP_SERVICE$$;
}
