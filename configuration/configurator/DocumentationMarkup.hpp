#ifndef _DOCUMENTATION_MARKUP_HPP
#define _DOCUMENTATION_MARKUP_HPP

static const char* DM_SECT2 = "sect2";
static const char* DM_SECT2_END = "</sect2>\n";
static const char* DM_SECT3_BEGIN = "";
static const char* DM_SECT3_END = "";
static const char* DM_SECT4_BEGIN = "";
static const char* DM_SECT4_END = "";
static const char* DM_ID = "id";
static const char* DM_PARA = "para";
static const char* DM_PARA_BEGIN = "<para><emphasis role=\"bold\">";
static const char* DM_PARA_END = "</emphasis></para>\n";
static const char* DM_LINE_BREAK = "<?linebreak?>";
static const char* DM_LINE_BREAK2 = "_<?linebreak?>";
static const char* DM_TITLE_BEGIN = DM_PARA_BEGIN;
static const char* DM_TITLE_END = DM_PARA_END;
static const char* DM_TITLE_LITERAL = "title";
static const char* DM_TABLE_BEGIN = "<informaltable colsep=\"1\" rowsep=\"1\" ";
static const char* DM_TABLE_ID_BEGIN = "id=\"";
static const char* DM_TABLE_ID_UNDEFINED = "UNDEFINED";
static const char* DM_TABLE_ID_END = "\">\n";
static const char* DM_TABLE_END = "</informaltable>\n";
static const char* DM_TABLE_ROW = "row";
static const char* DM_TABLE_ENTRY = "entry";
static const char* DM_TGROUP = "tgroup";
static const char* DM_TGROUP4_BEGIN = "<tgroup cols=\"4\" align=\"left\">\n";
static const char* DM_TGROUP4_END = "</tgroup>\n";
static const char* DM_TGROUP_END = "</tgroup>\n";
static const char* DM_TBODY_BEGIN = "<thead><row>\n<entry>attribute</entry>\n<entry>values</entry>\n<entry>default</entry>\n<entry>required</entry>\n</row>\n</thead><tbody>\n";
static const char* DM_TBODY_END = "</tbody>\n";
static const char* DM_COL_SPEC4  = "<colspec colwidth=\"155pt\" /><colspec colwidth=\"2*\" /><colspec colwidth=\"1*\" /><colspec colwidth=\"0.5*\" />\n";
static const char* DM_HEADING = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE sect1 PUBLIC \"-//OASIS//DTD DocBook XML V4.5//EN\"\n\"http://www.oasis-open.org/docbook/xml/4.5/docbookx.dtd\">\n";

#endif // _DOCUMENTATION_MARKUP_HPP
