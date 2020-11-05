#ifndef PRIMITIVE_TYPES_HPP__
#define PRIMITIVE_TYPES_HPP__

#include "jlib.hpp"
#include "jstring.hpp"
#include <string>

using namespace std;

namespace cppplugin
{

class Integer : public CInterface, implements IInterface
{
private:
    int v;
    StringBuffer s;
public:
    IMPLEMENT_IINTERFACE;

    Integer(int v_)
    {
        v = v_;
        s.appendf("%d", v_);
    }

    int val()
    {
        return v;
    }

    StringBuffer& str(StringBuffer& buf)
    {
        buf.append(s);
        return buf;
    }

    StringBuffer& str()
    {
        return s;
    }
};

class Integer64 : public CInterface, implements IInterface
{
private:
    int64_t v;
    StringBuffer s;
public:
    IMPLEMENT_IINTERFACE;

    Integer64(int64_t v_)
    {
        v = v_;
        s.appendf("%ld", v_);
    }

    int64_t val()
    {
        return v;
    }

    StringBuffer& str(StringBuffer& buf)
    {
        buf.append(s);
        return buf;
    }

    StringBuffer& str()
    {
        return s;
    }
};

class Float : public CInterface, implements IInterface
{
private:
    float v;
    StringBuffer s;
public:
    IMPLEMENT_IINTERFACE;

    Float(float v_)
    {
        v = v_;
        s.appendf("%f", v_);
    }

    float val()
    {
        return v;
    }

    StringBuffer& str(StringBuffer& buf)
    {
        buf.append(s);
        return buf;
    }

    StringBuffer& str()
    {
        return s;
    }
};

class Double : public CInterface, implements IInterface
{
private:
    double v;
    StringBuffer s;
public:
    IMPLEMENT_IINTERFACE;

    Double(double v_)
    {
        v = v_;
        s.appendf("%f", v_);
    }

    double val()
    {
        return v;
    }

    StringBuffer& str(StringBuffer& buf)
    {
        buf.append(s);
        return buf;
    }

    StringBuffer& str()
    {
        return s;
    }
};

class Boolean : public CInterface, implements IInterface
{
private:
    bool v;
    StringBuffer s;
public:
    IMPLEMENT_IINTERFACE;

    Boolean(bool v_)
    {
        v = v_;
        s.appendf("%s", v_?"true":"false");
    }

    bool val()
    {
        return v;
    }

    StringBuffer& str(StringBuffer& buf)
    {
        buf.append(s);
        return buf;
    }

    StringBuffer& str()
    {
        return s;
    }
};

class PString : public CInterface, implements IInterface
{
private:
    StringBuffer v;
public:
    IMPLEMENT_IINTERFACE;

    PString(const char* v_)
    {
        v.append(v_);
    }

    PString(size32_t len, const char* v_)
    {
        v.append(len, v_);
    }

    const char* val()
    {
        return v.str();
    }

    StringBuffer& str(StringBuffer& buf)
    {
        buf.append(v);
        return buf;
    }

    StringBuffer& str()
    {
        return v;
    }
};
}
#endif