#ifndef _ESPWIZ_WsELK_HPP_
#define _ESPWIZ_WsELK_HPP_

#include "ws_elk_esp.ipp"

class Cws_elkEx : public Cws_elk
{
private:
    Owned<IPropertyTree> m_serviceCfg;
public:
    IMPLEMENT_IINTERFACE

    Cws_elkEx();
    virtual ~Cws_elkEx();
    virtual void init(IPropertyTree *cfg, const char *process, const char *service);
    virtual bool onGetConfigDetails(IEspContext &context, IEspGetConfigDetailsRequest &req, IEspGetConfigDetailsResponse &resp);
};

#endif // _ESPWIZ_WsELK_HPP_
