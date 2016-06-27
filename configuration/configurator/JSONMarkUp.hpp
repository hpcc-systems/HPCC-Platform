#ifndef _JSONMARKUP_HPP_
#define _JSONMARKUP_HPP_

namespace CONFIGURATOR
{

/*static const char* JSON_BEGIN("\
{\n\
    \"data\": \n\
    {\n\
        \"PageTitle\": \"Configuration Manager\",\n\
        \"APIVersion\": \"2.0\",\n\
        \"toplevelcontainer\": \n\
        [\n");

static const char* JSON_END("\n\
        ]\n\
    }\n\
}\n");*/

static const char* JSON_BEGIN("\
{\n\
    \"content\": {\n\
        \"innercontent\": [\n");

static const char* JSON_END("\
                       ]\n\
    }\n\
}\n");

static const char* JSON_CONTENT_BEGIN(",\"content\" : {\n");

static const char* JSON_CONTENT_END("}\n");

static const char* JSON_LABEL("\"label\":");
static const char* JSON_VALUES("\"values\":");
static const char* JSON_VALUE("\"value\":");
static const char* JSON_TYPE("\"type\":");
static const char* JSON_TYPE_TAB("tab");
static const char* JSON_TYPE_TABLE("table");
static const char* JSON_TYPE_INPUT("input");
static const char* JSON_TYPE_DROP_DOWN("dropdown");
static const char* JSON_KEY("\"key\":");
static const char* JSON_TOOLTIP("\"tooltip\": ");
static const char* JSON_DEFAULT_VALUE("\"defaultValue\":");
static const char* JSON_COLUMN_NAMES_VALUE("\"columnNames\":");
static const char* JSON_INNER_CONTENT_BEGIN_1("\"innercontent\": [ \n");
static const char* JSON_INNER_CONTENT_END("]");

static const char* JSON_NAVIGATOR_BEGIN("[{\n");
static const char* JSON_NAVIGATOR_END("}]\n");
static const char* JSON_NAVIGATOR_NODE_BEGIN("[{\n");
static const char* JSON_NAVIGATOR_NODE_END("}]\n");
static const char* JSON_NAVIGATOR_TEXT("\"text\":");
//static const char* JSON_NAVIGATOR_TYPE("\"type\":");
static const char* JSON_NAVIGATOR_KEY("\"href\": ");
//static const char* JSON_NAVIGATOR_KEY(JSON_DEFAULT_VALUE);
static const char* JSON_NAVIGATOR_SELECTABLE("\"selectable\":");
static const char* JSON_NAVIGATOR_NODES("\"nodes\":");


#define CONTENT_INNER_CONTENT_BEGIN strJSON.append(JSON_CONTENT_BEGIN);offset += STANDARD_OFFSET_1;QuickOutPad(strJSON, offset);strJSON.append(JSON_INNER_CONTENT_BEGIN_1);
#define INNER_CONTENT_END offset -= STANDARD_OFFSET_1;strJSON.append(JSON_INNER_CONTENT_END);
#define CONTENT_CONTENT_END offset -= STANDARD_OFFSET_1;strJSON.append(JSON_CONTENT_END);

class CJSONMarkUpHelper
{

public:
    static void createUIContent(::StringBuffer &strJSON, unsigned int &offset, const char *pUIType, const char* pLabel, const char* pKey, const char *pToolTip = "", const char *pDefaultValue = "", const char* pValues = "", const char*  pValue = "");
    //static void createUIContent(::StringBuffer &strJSON, unsigned int &offset, ::StringBuffer strUIType, ::StringBuffer strLabel, ::StringBuffer strKey, ::StringBuffer strToolTip = "", ::StringBuffer strDefaultValue = "", ::StringBuffer strValues = "", ::StringBuffer strValue = "");
    static void getNavigatorJSON(::StringBuffer &strJSON);
private:
    static void markUpString(::StringBuffer &str);
};
}
#endif // _JSONMARKUP_HPP_
