#ifndef _ESPWIZ_$$ESP_SERVICE$$_HPP__
#define _ESPWIZ_$$ESP_SERVICE$$_HPP__

#include "$$root$$.esp"

class C$$ESP_SERVICE$$Ex : public C$$ESP_SERVICE$$
{
public:
   IMPLEMENT_IINTERFACE;

    virtual void init(IPropertyTree *cfg, const char *process, const char *service);

    bool on$$ESP_METHOD$$(IEspContext &context, IEsp$$ESP_METHOD$$Request &req, IEsp$$ESP_METHOD$$Response &resp);
};

#endif //_ESPWIZ_$$ESP_SERVICE$$_HPP__

