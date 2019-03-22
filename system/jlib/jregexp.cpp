/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */


#include "jlib.hpp"
#include "jmisc.hpp"
#include "jregexp.hpp"

#define FAIL(s) { /*assert(!s);*/ return NULL; }

#define SUBPARENL '{'
#define SUBPARENR '}'
// NB Meta must change if the above change

typedef unsigned REGFLAGS;

#define MAXEXPR  0xf000
#define MAXSUBST 0xf000
#define NSUBEXP  10
class RECOMP
{
public:
    const char *s_start[NSUBEXP];
    const char *s_end[NSUBEXP];
    const char *s_save; // save text after find
    const char *s_savestr[NSUBEXP];
    size32_t      s_savelen[NSUBEXP];
    char start;     
    char anch;      
    char *must;     
    size32_t mlen;      
    char *program;  
    const char *parse;      /* Input-scan pointer. */
    int npar;       /* {} count. */
    char *code;     /* Code-emit pointer. */
    const char *input;    /* String-input pointer. */
    const char *bol;    /* Beginning of input, for ^ check. */
    const char **startp;  /* Pointer to startp array. */
    const char **endp;    /* Ditto for end. */

    char * reg(bool sub, REGFLAGS &);
    char *branch (REGFLAGS &);
    char *piece (REGFLAGS &);
    char *atom (REGFLAGS &);
    char *rnode (char);
#ifdef _DEBUG
    void regc (char b) {
      if (code-program>=MAXEXPR-3)
        assertex(!"regular expression too complicated!");
      *code++ = b;
    };
#endif
    void insert (char, char *);
    void tail (char *, char *);
    void optail (char *, char *);
    char *next (char *);

    bool match (char *prog);
    char *rtry (const char *);
    int repeat (char *);

    const char * findstart;
    const char * findend;
    size32_t replacemax;     // needed for replace (clarion)
    bool nocase;
    const char *rstrchr(const char *s,int c);
    bool  strnequ(const char *s1,const char *s2,size32_t n);
    bool  eob(const char *s) { return s==findend; };
    bool  claeol(const char *s);
    bool  equch(int c1,int c2) { if (nocase) return (toupper(c1)==toupper(c2)); return c1==c2; };
    void  adjustptrs(const char *e,size32_t lo,size32_t lr);
    size32_t rstrlen(const char *s) {size32_t l=0; while (s++!=findend) l++; return l; };
    bool clarion;
    bool suboverlap(int n,int m);

    RECOMP() { _clear(s_start); s_save=NULL; program = NULL; };
    ~RECOMP() {
      free((void *)s_save);
      free((void *)program);
    }
};

#ifndef _DEBUG
  #define REGC(b) *code++ = b
#else
  #define REGC(b) regc(b)
#endif


//----

/*
 * The "internal use only" fields in regexp.h are present to pass info from
 * compile to execute that permits the execute phase to run lots faster on
 * simple cases.  They are:
 *
 * regstart char that must begin a match; '\0' if none obvious
 * reganch  is the match anchored (at beginning-of-line only)?
 * regmust  string (pointer into program) that match must include, or NULL
 * regmlen  length of regmust string
 *
 * Regstart and reganch permit very fast decisions on suitable starting points
 * for a match, cutting down the work a lot.  Regmust permits fast rejection
 * of lines that cannot possibly match.  The regmust tests are costly enough
 * that regcomp() supplies a regmust only if the r.e. contains something
 * potentially expensive (at present, the only such thing detected is * or +
 * at the start of the r.e., which can involve a lot of backup).  Regmlen is
 * supplied because the test in regexec() needs it and regcomp() is computing
 * it anyway.
 */

/*
 * Structure for regexp "program".  This is essentially a linear encoding
 * of a nondeterministic finite-state machine (aka syntax charts or
 * "railroad normal form" in parsing technology).  Each node is an opcode
 * plus a "next" pointer, possibly plus an operand.  "Next" pointers of
 * all nodes except BRANCH implement concatenation; a "next" pointer with
 * a BRANCH on both ends of it is connecting two alternatives.  (Here we
 * have one of the subtle syntax dependencies:  an individual BRANCH (as
 * opposed to a collection of them) is never concatenated with anything
 * because of operator precedence.)  The operand of some types of node is
 * a literal string; for others, it is a node leading into a sub-FSM.  In
 * particular, the operand of a BRANCH node is the first node of the branch.
 * (NB this is *not* a tree structure:  the tail of the branch connects
 * to the thing following the set of BRANCHes.)  The opcodes are:
 */

/* definition   number  opnd?   meaning */
#define END 0   /* no   End of program. */
#define BOL 1   /* no   Match "" at beginning of line. */
#define EOL 2   /* no   Match "" at end of line. */
#define ANY 3   /* no   Match any one character. */
#define ANYOF   4   /* str  Match any character in this string. */
#define ANYBUT  5   /* str  Match any character not in this string. */
#define BRANCH  6   /* node Match this alternative, or the next... */
#define BACK    7   /* no   Match "", "next" ptr points backward. */
#define EXACTLY 8   /* str  Match this string. */
#define NOTHING 9   /* no   Match empty string. */
#define STAR    10  /* node Match this (simple) thing 0 or more times. */
#define PLUS    11  /* node Match this (simple) thing 1 or more times. */
#define OPEN    20  /* no   Mark this point in input as start of #n. */
            /*  OPEN+1 is number 1, etc. */
#define CLOSE   30  /* no   Analogous to OPEN. */

/*
 * Opcode notes:
 *
 * BRANCH   The set of branches constituting a single choice are hooked
 *      together with their "next" pointers, since precedence prevents
 *      anything being concatenated to any individual branch.  The
 *      "next" pointer of the last BRANCH in a choice points to the
 *      thing following the whole choice.  This is also where the
 *      final "next" pointer of each individual branch points; each
 *      branch starts with the operand node of a BRANCH node.
 *
 * BACK     Normal "next" pointers all implicitly point forward; BACK
 *      exists to make loop structures possible.
 *
 * STAR,PLUS    '?', and complex '*' and '+', are implemented as circular
 *      BRANCH structures using BACK.  Simple cases (one character
 *      per match) are implemented with STAR and PLUS for speed
 *      and to minimize recursive plunges.
 *
 * OPEN,CLOSE   ...are numbered at compile time.
 */

/*
 * A node is one char of opcode followed by two chars of "next" pointer.
 * "Next" pointers are stored as two 8-bit pieces, high order first.  The
 * value is a positive offset from the opcode of the node containing it.
 * An operand, if any, simply follows the node.  (Note that much of the
 * code generation knows about this implicit relationship.)
 *
 * Using two bytes for the "next" pointer is vast overkill for most things,
 * but allows patterns to get big without disasters.
 */
#define OP(p)   (*(p))
#define NEXT(p) (((*((p)+1)&0377)<<8) + (*((p)+2)&0377))
#define OPERAND(p)  ((p) + 3)



// Utility definitions.
#define UCHARAT(p)  ((int)*(unsigned char *)(p))

#define ISMULT(c)   ((c) == '*' || (c) == '+' || (c) == '?')
#define META    "^$.[{}|?+*\\"

// Flags to be passed up and down.
#define HASWIDTH    01  /* Known never to match null string. */
#define SIMPLE      02  /* Simple enough to be STAR/PLUS operand. */
#define SPSTART     04  /* Starts with * or +. */
#define WORST       0   /* Worst case. */



RegExpr::RegExpr()
{
  re = NULL;
}

RegExpr::~RegExpr()
{
  delete re;
}

RegExpr::RegExpr(const char *r,bool nocase)
{
  re = NULL;
  init(r,nocase);
}

bool RegExpr::init(const char *exp,bool nocase)
// Compiles the regular expression ready for Find
// if nocase = 1 the matching is case insensitive (where possible)
{
  kill();
  char *scan;
  char *longest;
  memsize_t len;
  REGFLAGS flags;
  assertex(exp!=NULL);
  delete re;
  re = new RECOMP;
  re->clarion = false;
  re->parse = exp;
  re->npar = 1;
  re->program = (char *)malloc(MAXEXPR);
  re->code = re->program;
  re->nocase = nocase;
  re->start = '\0';  /* Worst-case defaults. */
  re->anch = 0;
  re->must = NULL;
  re->mlen = 0;
  if (re->reg(0, flags) == NULL)
    return false;

  /* Dig out information for optimizations. */
  scan = re->program;      /* First BRANCH. */
  if (OP(re->next(scan)) == END) {    /* Only one top-level choice. */
    scan = OPERAND(scan);

    /* Starting-point info. */
    if (OP(scan) == EXACTLY)
      re->start = *OPERAND(scan);
    else if (OP(scan) == BOL)
      re->anch++;

    /*
     * If there's something expensive in the r.e., find the
     * longest literal string that must appear and make it the
     * regmust.  Resolve ties in favor of later strings, since
     * the regstart check works with the beginning of the r.e.
     * and avoiding duplication strengthens checking.  Not a
     * strong reason, but sufficient in the absence of others.
     */
    if (flags&SPSTART) {
      longest = NULL;
      len = 0;
      for (; scan != NULL; scan = re->next(scan))
        if (OP(scan) == EXACTLY && strlen(OPERAND(scan)) >= (size32_t) len) {
          longest = OPERAND(scan);
          len = strlen(OPERAND(scan));
        }
      re->must = longest;
      re->mlen = (size32_t)len;
    }
  }
  int reloc = 0;
  if (re->must)
    reloc = re->must-re->program;
  re->program = (char *)realloc(re->program,(re->code-re->program)+1);
  if (re->must)
    re->must = (char *)(re->program + reloc);
  return true;
}


const char * RegExpr::find(const char *string,size32_t from,size32_t len,size32_t maxlen)
// finds the first occurrence of the RE in string
{
  /* Be paranoid... */
  if (re == NULL || re->program== NULL) {
    assertex(!"NULL parameter");
    return NULL;
  }
  if (!string) { // find again
    if (re->findstart == NULL) {
      assertex(!"NULL parameter");
      return NULL;
    }
    string = (char *)re->s_end[0];
    if ((string==NULL)||(string == re->findend))
      return NULL;
  }
  else {
    re->findstart = string;
    string+=from;
    re->replacemax = maxlen;
    if (maxlen) {
      re->clarion = true;
      re->findend = re->findstart+maxlen;
    }
    else {
      const char *s=string;
      for (size32_t l=0;*s&&(l<len);l++) s++;
      re->findend = s;
    }
  }

  free((void *)re->s_save);
  re->s_save = NULL;    // clear saved (replaced string)


  /* If there is a "must appear" string, look for it. */
  if (re->must != NULL) {
    const char *s = string;
    while ((s = re->rstrchr(s, re->must[0])) != NULL) {
      if (re->strnequ(s, re->must, re->mlen))
        break;  /* Found it. */
      s++;
    }
    if (s == NULL)  /* Not present. */
      return 0;
  }

  /* Mark beginning of line for ^ . */
  re->bol = string;

  /* Simplest case:  anchored match need be tried only once. */
  char *ret = NULL;
  if (re->anch)
    ret = re->rtry(string);
  else {
    /* Messy cases:  unanchored match. */
    const char *s = string;
    if (re->start != '\0')
      /* We know what char it must start with. */
      while ((s = re->rstrchr(s, re->start)) != NULL) {
        if ((ret=re->rtry(s))!=NULL)
          break;
        s++;
      }
    else
      /* We don't -- general case. */
      do {
        if ((ret=re->rtry(s))!=NULL)
          break;
      } while (!re->eob(s++));
  }
  if (ret) {
    const char *ss = re->s_start[0];
    size32_t tl = re->s_end[0]-ss;
    char *ds=(char *)malloc(tl+1);
    memcpy(ds,ss,tl);
    ds[tl] = 0;
    for (int i = 0;i<NSUBEXP;i++) {
      const char *ss2 = re->s_start[i];
      if (ss2) {
        re->s_savelen[i]  = re->s_end[i]-ss2;
        re->s_savestr[i] = ds+(ss2-ss);
      }
      else {
        re->s_savelen[i] = 0;
        re->s_savestr[i] = NULL;
      }
    }
    re->s_save = ds;
  }
  else {
    _clear(re->s_start);
  }
  return ret;
}


size32_t RegExpr::findlen(unsigned n)
// size of string last matched using find
{
  if (re == NULL || re->program== NULL) {
    assertex(!"NULL parameter");
    return 0;
  }
  assertex(n<NSUBEXP);
  if ((n>=NSUBEXP)) return 0;
  if (re->s_save)
    return re->s_savelen[n];
  return 0;
}

const char *RegExpr::findstr(StringBuffer &s,unsigned n)
// returns string last matched (n = 0) or substring n (n>0)
{
  size32_t l = findlen(n);
  if (l && re->s_save) {
    s.append(l,re->s_savestr[n]);
  }
  return s.str();
}

const char *RegExpr::findnext()
{
  return find(NULL);
}

void RegExpr::replace(const char *rs,unsigned maxlen,unsigned n)
// replaces string (n = 0), or substring (n>0) by 's'
// in string at position previously found by Find or FindNext.
// Multiple replaces may be called on a single Find/FindNext

{
  if (re == NULL || re->program== NULL) {
    assertex(!"NULL parameter");
    return;
  }
  assertex(n<NSUBEXP);
  if ((n>=NSUBEXP)) return;
  const char *s=re->s_start[n];
  if (!s) return;
  if (maxlen==0) maxlen = re->replacemax;
  if (maxlen==0) return;
  const char *e=re->s_end[n];
  if (!e) return;
  size32_t lo=e-s;
  size32_t lt=re->rstrlen(e);
  size32_t lr = (size32_t)strlen(rs);
  if (lr>lo) {
    size32_t d = lr-lo;                   // delta
    size32_t l = ((e-re->findstart)+lt);  // total length
    size32_t r = maxlen;
    if (l>r) {
      assertex(!"replace - maxlen too small for passed string!");
      return;
    }
    r-=l;                               // r = max gap left
    if (!re->clarion) r--;              // for null
    if (r<d) {
      if (lt<d-r) {
        lr+=lt;
        if (lr<=(d-r)) return;          // can't really happen
        lr-=(d-r);                      // new string too big
        lt=0;                           // lose tail
      }
      else
        lt-=(d-r);                      // losing part of tail
    }
    if (lt) memmove((void *)(e+d),e,lt);
    if (!re->clarion) *((char *)(s+(lr+lt))) = 0; // terminate
  }
  else if (lr<lo) {
    size32_t d2 = lo-lr;                   // delta
    // must be enough room so fairly simple
    if (re->clarion) {
      memmove((void *)(e-d2),e,lt);
      memset((void *)(e+(lt-d2)),' ',d2);
    }
    else
      memmove((void *)(e-lo+lr),e,lt+1);
  }
  memcpy((void *)s,rs,lr);
  re->adjustptrs(e,lo,lr);
}

static void dosubstitute(RegExpr *re,StringBuffer &out,char *s)
{
  for (char* b= strchr(s,'#');b;b=strchr(b+1,'#')) {
    char c = b[1];
    if (c=='#') {
      *(++b)=0;
      out.append(s);
      s = b+1;
    }
    else if (!isdigit(c))
      continue;
    else {
      (*b++)=0;
      out.append(s);
      re->findstr(out,c-'0');
    }
    s = b+1;  // reset start
  }
  out.append(s);
}

const char * RegExpr::substitute(StringBuffer &out,const char *mask,...)
{
  char * str = (char *)malloc(MAXSUBST);
  va_list ap;
  va_start(ap,mask);
  vsprintf(str,mask,ap);
  va_end(ap);
  dosubstitute(this,out,str);
  free(str);
  return (char*)out.str();
}



void RegExpr::kill()
// releases extra storage used by RegularExpressionClass
{

  delete re;
  re = NULL;
}


char *RECOMP::reg (bool paren, REGFLAGS &flags)
{
  char *ret;
  char *br;
  char *ender;
  int parno = 0;

  flags = HASWIDTH;  /* Tentatively. */


  /* Make an OPEN node, if parenthesized. */
  if (paren) {
    if (npar >= NSUBEXP)
      FAIL("too many {}");
    parno = npar;
    npar++;
    ret = rnode((char)(OPEN+parno));
  } else
    ret = NULL;

  /* Pick up the branches, linking them together. */
  REGFLAGS bflags;
  br = branch(bflags);
  if (br == NULL)
    return(NULL);
  if (ret != NULL)
    tail(ret, br);  /* OPEN -> first. */
  else
    ret = br;
  if (!(bflags&HASWIDTH))
    flags &= ~HASWIDTH;
  flags |= bflags&SPSTART;
  while (*parse == '|') {
    parse++;
    br = branch(bflags);
    if (br == NULL)
      return(NULL);
    tail(ret, br);  /* BRANCH -> BRANCH. */
    if (!(bflags&HASWIDTH))
      flags &= ~HASWIDTH;
    flags |= bflags&SPSTART;
  }

  /* Make a closing node, and hook it on the end. */
  ender = rnode((char)((paren) ? CLOSE+parno : END));
  tail(ret, ender);

  /* Hook the tails of the branches to the closing node. */
  for (br = ret; br != NULL; br = next(br))
    optail(br, ender);

  /* Check for proper termination. */
  if (paren && *parse++ != SUBPARENR) {
    FAIL("unmatched {}");
  } else if (!paren && *parse != '\0') {
    if (*parse == SUBPARENR) {
      FAIL("unmatched {}");
    } else
      FAIL("junk on end");  /* "Can't happen". */
    /* NOTREACHED */
  }

  return(ret);
}


char *RECOMP::branch (REGFLAGS &flags)
/*
 - branch - one alternative of an | operator
 *
 * Implements the concatenation operator.
 */
{
  char *ret;
  char *chain;
  char *latest;

  flags = WORST;    /* Tentatively. */

  ret = rnode(BRANCH);
  chain = NULL;
  REGFLAGS pflags;
  while (*parse != '\0' && *parse != '|' && *parse != SUBPARENR) {
    latest = piece(pflags);
    if (latest == NULL)
      return(NULL);
    flags |= pflags&HASWIDTH;
    if (chain == NULL)  /* First piece. */
      flags |= pflags&SPSTART;
    else
      tail(chain, latest);
    chain = latest;
  }
  if (chain == NULL)  /* Loop ran zero times. */
    (void) rnode(NOTHING);

  return(ret);
}

char *RECOMP::piece (REGFLAGS &flags)
/*
 - piece - something followed by possible [*+?]
 *
 * Note that the branching code sequences used for ? and the general cases
 * of * and + are somewhat optimized:  they use the same NOTHING node as
 * both the endmarker for their branch list and the body of the last branch.
 * It might seem that this node could be dispensed with entirely, but the
 * endmarker role is not redundant.
 */
{
  char *ret;
  char op;
  char *next;

  REGFLAGS aflags;
  ret = atom(aflags);
  if (ret == NULL)
    return(NULL);

  op = *parse;
  if (!ISMULT(op)) {
    flags = aflags;
    return(ret);
  }

  if (!(aflags&HASWIDTH) && op != '?')
    FAIL("*+ operand could be empty");
  flags = (op != '+') ? (WORST|SPSTART) : (WORST|HASWIDTH);

  if (op == '*' && (aflags&SIMPLE))
    insert(STAR, ret);
  else if (op == '*') {
    /* Emit x* as (x&|), where & means "self". */
    insert(BRANCH, ret);      /* Either x */
    optail(ret, rnode(BACK));    /* and loop */
    optail(ret, ret);      /* back */
    tail(ret, rnode(BRANCH));    /* or */
    tail(ret, rnode(NOTHING));    /* null. */
  } else if (op == '+' && (aflags&SIMPLE))
    insert(PLUS, ret);
  else if (op == '+') {
    /* Emit x+ as x(&|), where & means "self". */
    next = rnode(BRANCH);      /* Either */
    tail(ret, next);
    tail(rnode(BACK), ret);    /* loop back */
    tail(next, rnode(BRANCH));    /* or */
    tail(ret, rnode(NOTHING));    /* null. */
  } else if (op == '?') {
    /* Emit x? as (x|) */
    insert(BRANCH, ret);      /* Either x */
    tail(ret, rnode(BRANCH));    /* or */
    next = rnode(NOTHING);    /* null. */
    tail(ret, next);
    optail(ret, next);
  }
  parse++;
  if (ISMULT(*parse))
    FAIL("nested *?+");

  return(ret);
}

char *RECOMP::atom (REGFLAGS &flags)
/*
 - atom - the lowest level
 *
 * Optimization:  gobbles an entire sequence of ordinary characters so that
 * it can turn them into a single node, which is smaller to store and
 * faster to run.  Backslashed characters are exceptions, each becoming a
 * separate node; the code is simpler that way and it's not worth fixing.
 */
{
  char *ret;

  flags = WORST;    /* Tentatively. */

  switch (*parse++) {
  case '^':
    ret = rnode(BOL);
    break;
  case '$':
    ret = rnode(EOL);
    break;
  case '.':
    ret = rnode(ANY);
    flags |= HASWIDTH|SIMPLE;
    break;
  case '[': {
      int clss;
      int clssend;

      if ((*parse == '^') || (*parse == '~')) {  /* Complement of range. */
        ret = rnode(ANYBUT);
        parse++;
      } else
        ret = rnode(ANYOF);
      if (*parse == ']' || *parse == '-')
        REGC(*parse++);
      while (*parse != '\0' && *parse != ']') {
        if (*parse == '-') {
          parse++;
          if (*parse == ']' || *parse == '\0')
            REGC('-');
          else {
            clss = UCHARAT(parse-2)+1;
            clssend = UCHARAT(parse);
            if (clss > clssend+1)
              FAIL("invalid [] range");
            for (; clss <= clssend; clss++)
              REGC((char) clss);
            parse++;
          }
        } else
          REGC(*parse++);
      }
      REGC('\0');
      if (*parse != ']')
        FAIL("unmatched []");
      parse++;
      flags |= HASWIDTH|SIMPLE;
    }
    break;
  case SUBPARENL: {
      REGFLAGS sflags;
      ret = reg(1, sflags);
      if (ret == NULL)
        return(NULL);
      flags |= sflags&(HASWIDTH|SPSTART);
    }
    break;
  case '\0':
  case '|':
  case SUBPARENR:
    FAIL("internal urp");  /* Supposed to be caught earlier. */
    break;
  case '?':
  case '+':
  case '*':
    FAIL("?+* follows nothing");
    break;
  case '\\':
    if (*parse == '\0')
      FAIL("trailing \\");
    ret = rnode(EXACTLY);
    REGC(*parse++);
    REGC('\0');
    flags |= HASWIDTH|SIMPLE;
    break;
  default: {
      memsize_t len;
      char ender;

      parse--;
      len = strcspn(parse, META);
      if (len <= 0)
        FAIL("internal disaster");
      ender = *(parse+len);
      if (len > 1 && ISMULT(ender))
        len--;    /* Back off clear of ?+* operand. */
      flags |= HASWIDTH;
      if (len == 1)
        flags |= SIMPLE;
      ret = rnode(EXACTLY);
      while (len > 0) {
        REGC(*parse++);
        len--;
      }
      REGC('\0');
    }
    break;
  }

  return(ret);
}

char *RECOMP::rnode (char op)
/*
 - rnode - emit a node
 */
{
  char *ret = code;
  REGC(op);
  REGC(0);
  REGC(0);
  return(ret);
}



void RECOMP::insert (char op, char *opnd)
/*
 - insert - insert an operator in front of already-emitted operand
 *
 * Means relocating the operand.
 */
{
  char *src=code;
  code+=3;
  char *dst=code;
  while (src > opnd)
    *--dst = *--src;
  char *place = (char *)opnd;    /* Op node, where operand used to be. */
  *place++ = op;
  *place++ = '\0';
  *place++ = '\0';
}

void RECOMP::tail (char *p, char *val)
/*
 - tail - set the next-pointer at the end of a node chain
 */
{
  char *scan;
  char *temp;
  int offset;

  /* Find last node. */
  scan = p;
  for (;;) {
    temp = next(scan);
    if (temp == NULL)
      break;
    scan = temp;
  }

  if (OP(scan) == BACK)
    offset = scan - val;
  else
    offset = val - scan;
  *(scan+1) = (char) ((offset>>8)&0377);
  *(scan+2) = (char) (offset&0377);
}

void RECOMP::optail (char *p, char *val)
/*
 - optail - tail on operand of first argument; nop if operandless
 */
{
  /* "Operandless" and "op != BRANCH" are synonymous in practice. */
  if (p == NULL || OP(p) != BRANCH)
    return;
  tail(OPERAND(p), val);
}

char *RECOMP::next (char *p)
/*
 - next - dig the "next" pointer out of a node
 */
{
  int offset = NEXT(p);
  if (offset == 0)
    return(NULL);

  if (OP(p) == BACK)
    return(p-offset);
  else
    return(p+offset);
}

/*
 - rtry - try match at specific point
 */
char * RECOMP::rtry (const char *string)
{
  input = string;
  startp = s_start;
  endp = s_end;
  _clear(s_start);
  _clear(s_end);
  if (match(program)) {
    s_start[0] = string;
    s_end[0] = input;
    return (char *)string;
  }
  return NULL;
}


bool RECOMP::match (char *prog)
{
  char *scan;  /* Current node. */
  char *n;    /* Next node. */

  scan = prog;
  while (scan != NULL) {
    n = next(scan);

    switch (OP(scan)) {
    case BOL:
      if (input != bol)
        return false;
      break;
    case EOL:
      if (clarion) {
        if (!claeol(input))
          return false;
      }
      else if (!eob(input))
        return false;
      break;
    case ANY:
      if (eob(input))
        return false;
      input++;
      break;
    case EXACTLY: {
        memsize_t len;
        char *opnd;

        opnd = OPERAND(scan);
        /* Inline the first character, for speed. */
        if (!equch(*opnd,*input))
          return false;
        len = strlen(opnd);
        if (len > 1 && !strnequ(input, opnd, (size32_t)len))
          return false;
        input += len;
      }
      break;
    case ANYOF:
       if (eob(input) || strchr(OPERAND(scan), *input) == NULL)
        return false;
      input++;
      break;
    case ANYBUT:
       if (eob(input) || strchr(OPERAND(scan), *input) != NULL)
        return false;
      input++;
      break;
    case NOTHING:
      break;
    case BACK:
      break;
    case OPEN+1:
    case OPEN+2:
    case OPEN+3:
    case OPEN+4:
    case OPEN+5:
    case OPEN+6:
    case OPEN+7:
    case OPEN+8:
    case OPEN+9: {
        int no;
        const char *save;

        no = OP(scan) - OPEN;
        save = input;

        if (match(n)) {
          /*
           * Don't set start if some later
           * invocation of the same parentheses
           * already has.
           */
          if (startp[no] == NULL)
            startp[no] = save;
          return true;
        } else
          return false;
      }
      break;
    case CLOSE+1:
    case CLOSE+2:
    case CLOSE+3:
    case CLOSE+4:
    case CLOSE+5:
    case CLOSE+6:
    case CLOSE+7:
    case CLOSE+8:
    case CLOSE+9: {
        int no;
        const char *save;

        no = OP(scan) - CLOSE;
        save = input;

        if (match(n)) {
          /*
           * Don't set end if some later
           * invocation of the same parentheses
           * already has.
           */
          if (endp[no] == NULL)
            endp[no] = save;
          return true;
        } else
          return false;
      }
      break;
    case BRANCH: {
        const char *save;

        if (OP(n) != BRANCH)    /* No choice. */
          n = OPERAND(scan);  /* Avoid recursion. */
        else {
          do {
            save = input;
            if (match(OPERAND(scan)))
              return true;
            input = save;
            scan = next(scan);
          } while (scan != NULL && OP(scan) == BRANCH);
          return false;
          /* NOTREACHED */
        }
      }
      break;
    case STAR:
    case PLUS: {
        char nextch;
        int no;
        const char *save;
        int min;

        /*
         * Lookahead to avoid useless match attempts
         * when we know what character comes n.
         */
        nextch = '\0';
        if (OP(n) == EXACTLY)
          nextch = *OPERAND(n);
        min = (OP(scan) == STAR) ? 0 : 1;
        save = input;
        no = repeat(OPERAND(scan));
        while (no >= min) {
          /* If it could work, try it. */
          if (nextch == '\0' || equch(*input,nextch))
            if (match(n))
              return true;
          /* Couldn't or didn't -- back up. */
          no--;
          input = save + no;
        }
        return false;
      }
      break;
    case END:
      return true;  /* Success! */
      break;
    default:
      assertex(!"memory corruption");
      return false;
      break;
    }

    scan = n;
  }

  /*
   * We get here only if there's trouble -- normally "case END" is
   * the terminating point.
   */
  assertex(!"corrupted pointers");
  return false;
}

int RECOMP::repeat (char *p)
/*
 - regrepeat - repeatedly match something simple, report how many
 */
{
  int count = 0;
  const char *scan = input;
  char *opnd = OPERAND(p);
  switch (OP(p)) {
  case ANY:
    count = rstrlen(scan);
    scan += count;
    break;
  case EXACTLY:
    while (equch(*opnd,*scan)) {
      count++;
      scan++;
    }
    break;
  case ANYOF:
    while (!eob(scan) && strchr(opnd, *scan) != NULL) { // NB not rstrchr!
      count++;
      scan++;
    }
    break;
  case ANYBUT:
    while (!eob(scan) && strchr(opnd, *scan) == NULL) { // NB nnot rstrchr
      count++;
      scan++;
    }
    break;
  default:    /* Oh dear.  Called inappropriately. */
    assertex(!"internal foulup");
    count = 0;  /* Best compromise. */
    break;
  }
  input = scan;

  return(count);
}

void RECOMP::adjustptrs(const char *e,size32_t lo,size32_t lr)
{
  if (lo==lr) return;
  size32_t o=e-findstart;
  for (int i=0;i<NSUBEXP;i++) {
    const char *s= s_start[i];
    if (s) {
      size32_t so = s-findstart;
      if (so>=o) {
        s_start[i]=s+lr-lo;
      }
      so = s_end[i]-findstart;
      if (so>=o)
        s_end[i]+=lr-lo;
    }
  }
  findend+=(lr-lo);
}


const char *RECOMP::rstrchr(const char *s,int c)
{
  if (nocase) {
    c = toupper(c);
    while (s!=findend) {
      if (toupper(*s)==c) return s;
      s++;
    }
  }
  else {
    while (s!=findend) {
      if (*s==c) return s;
      s++;
    }
  }
  return NULL;
}

bool RECOMP::strnequ(const char *s1,const char *s2,size32_t n)
{ // s1 is in regsearch
  if (findend-s1 < (signed)n) return false;
  if (nocase) {
    while (n--) {
      if (toupper(*s1)!=toupper(*s2)) return false;
      s1++;
      s2++;
    }
  }
  else {
    while (n--) {
      if (*s1!=*s2) return false;
      s1++;
      s2++;
    }
  }
  return true;
}

bool RECOMP::claeol(const char *s)
{
  do {
    if (eob(s)) return true;
  }
  while ((*(s++))==' ');
  return false;
}

unsigned char xlat[26]={0,'1','2','3',0,'1','2',0,0,'2','2','4','5','5',0,'1','2','6','2','3',0,'1',0,'2',0,'2'};

static char *SoundexCode(const char *s,int l,char *res)
{
  char *r=res;
  r[1] = '0';
  r[2] = '0';
  r[3] = '0';
  r[4] = 0;
  for (;;) {
    if (!l||!*s) {
      *r = '!';
      return res;
    }
    if (isalpha(*s)) break;
    s++;
    l--;
  }
  char c=toupper(*s);
  *r = c;
  r++; s++;
  char dl = ((c>='A')&&(c<='Z'))?xlat[c-'A']:0;
  while (l&&*s) {
    c = toupper(*s);
    s++;
    l--;
    if ((c>='A')&&(c<='Z')) {
      char d = xlat[c-'A'];
      if (d) {
            if(d!=dl) {
                *r = d;
                r++;
                if (!*r) break;
            }
      }
      else if (c != 'H' && c != 'W')
          dl = d;
    }
  }
  return res;
}

//---------------------------------------------------------------------------------------------------------------------

inline bool matches(char cur, char next, bool nocase)
{
    return (nocase ? (toupper(cur)==toupper(next)) : cur == next);
}

/* Search for a pattern pat anywhere within the search string src */
static bool WildSubStringMatch(const char *src, size_t srclen, const char *pat, size_t patlen, bool nocase)
{
    //On entry the pattern to match contains at least one leading non '*' character
    char pat0 = pat[0];
    if (nocase)
        pat0 = toupper(pat[0]);

    //Could special case '?' at the start of the string, but fairly unlikely.
    for (size_t srcdelta=0; srcdelta < srclen; srcdelta++)
    {
        size_t patidx=0;
        size_t srcidx = srcdelta;
        if (likely(pat0 != '?'))
        {
            //Quick scan to find a match for the first character
            if (!nocase)
            {
                for (;;)
                {
                    if (unlikely(src[srcdelta] == pat0))
                        break;
                    srcdelta++;
                    if (unlikely(srcdelta == srclen))
                        return false;
                }
            }
            else
            {
                for (;;)
                {
                    if (unlikely(toupper(src[srcdelta]) == pat0))
                        break;
                    srcdelta++;
                    if (unlikely(srcdelta == srclen))
                        return false;
                }
            }
            patidx=1;
            srcidx = srcdelta+1;
        }
        for (;;)
        {
            if (patidx == patlen)
                return true;

            char next = pat[patidx];
            if (next == '*')
            {
                do
                {
                    patidx++;
                }
                while ((patidx < patlen) && (pat[patidx] == '*'));
                dbgassertex((patidx != patlen)); // pattern should never finish with a '*'
                if (WildSubStringMatch(src+srcidx, srclen-srcidx, pat+patidx, patlen-patidx, nocase))
                    return true;
                break; // retry at next position
            }
            if (srcidx == srclen)
                break; // retry at next position
            if (next != '?')
            {
                char cur = src[srcidx];
                if (!matches(cur, next, nocase))
                    break; // retry at next position
            }
            patidx++;
            srcidx++;
        }
    }
    return false;
}

static bool WildMatchN ( const char *src, size_t srclen, size_t srcidx,
                    const char *pat, size_t patlen, size_t patidx, bool nocase)
{
    //First check for matching prefix
    char next;
    while (patidx < patlen)
    {
        next = pat[patidx];
        if (next == '*')
            break;
        if (srcidx >= srclen)
            return false;
        if (next != '?')
        {
            if (!matches(src[srcidx], next, nocase))
                return false;
        }
        srcidx++;
        patidx++;
    }

    //Now check for matching suffix
    while (patidx < patlen)
    {
        next = pat[patlen-1];
        if (next == '*')
            break;
        if (srcidx >= srclen)
            return false;
        if (next != '?')
        {
            if (!matches(src[srclen-1], next, nocase))
                return false;
        }
        srclen--;
        patlen--;
    }

    //String contains no wildcards...
    if (patidx == patlen)
        return (srcidx == srclen);

    dbgassertex(pat[patidx] == '*');
    dbgassertex(pat[patlen-1] == '*');

    //Skip multiple wildcards on the prefix and suffix.
    while (patidx < patlen && pat[patidx] == '*')
        patidx++;
    while (patidx < patlen && pat[patlen-1] == '*')
        patlen--;

    //abc*def
    if (patidx == patlen)
        return true;

    //Must match at least one character, if no characters left in the search string, then it fails to match
    if (srcidx == srclen)
        return false;

    //Search for the remaining pattern at an arbitrary position with the search string
    return WildSubStringMatch(src+srcidx, srclen-srcidx, pat+patidx, patlen-patidx, nocase);
}

bool jlib_decl WildMatch(const char *src, size_t srclen, const char *pat, size_t patlen, bool nocase)
{
  return WildMatchN(src,srclen,0,pat,patlen,0,nocase);
}

bool jlib_decl WildMatch(const char *src, const char *pat, bool nocase)
{
    //This could match constant prefixes before calling strlen(), but unlikely to be very significant
    return WildMatchN(src, strlen(src), 0, pat, strlen(pat), 0, nocase);
}

bool jlib_decl containsWildcard(const char * pattern)
{
    for (;;)
    {
        char c = *pattern++;
        switch (c)
        {
        case 0:
            return false;
        case '?':
        case '*':
            return true;
        }
    }
}


static bool WildMatchNreplace ( const char *src, int srclen, int srcidx,
                               const char *pat, int patlen, int patidx,
                               int nocase,
                               UnsignedArray &wild, UnsignedArray &wildlen
                               )
{
    unsigned wl = wild.ordinality();
    char next_char;
    for (;;) {
        if (patidx == patlen) {
            if (srcidx == srclen)
                return true;
            goto Fail;
        }
        next_char = pat[patidx++];
        if (next_char == '?') {
            if (srcidx == srclen)
                goto Fail;
            wild.append(srcidx++);
            wildlen.append(1);
        }
        else if (next_char != '*') {
            if (nocase) {
                if ((srcidx == srclen) ||
                    (toupper(src[srcidx])!=toupper(next_char)))
                    goto Fail;
            }
            else {
                if ((srcidx == srclen) || (src[srcidx]!=next_char))
                    goto Fail;
            }
            srcidx++;
        }
        else {
            for (;;) {
                if (patidx == patlen) {
                    wild.append(srcidx);
                    wildlen.append(srclen-srcidx);
                    return true;
                }
                wild.append(srcidx);    
                wildlen.append(0);              // placemarker
                if (pat[patidx] != '*') 
                    break;
                patidx++; // someone being silly!
            }
            while (srcidx < srclen) {
                if (WildMatchNreplace(src,srclen,srcidx,
                    pat, patlen, patidx,nocase, wild,wildlen))
                    return true;
                wildlen.append(wildlen.popGet()+1); // prob can do a bit better than this!
                srcidx++;
            }
            break; // fail
        }
    }
Fail:
    unsigned pn = wild.ordinality()-wl;
    if (pn) {
        wild.popn(pn);
        wildlen.popn(pn);
    }
    return false;
}

bool jlib_decl WildMatchReplace(const char *src, const char *pat, const char *repl, bool nocase, StringBuffer &out)
{
    UnsignedArray wild;
    UnsignedArray wildlen;
    if (!WildMatchNreplace(src,(size32_t)strlen(src),0,pat,(size32_t)strlen(pat),0,nocase,wild,wildlen))
        return false;
    for (;;) {
        char c = *(repl++);
        if (!c)
            break;
        if (c!='$')
            out.append(c);
        else {
            char c2 = *(repl++);
            if (!c2) {
                out.append(c);
                break;
            }
            if (c2=='$') 
                out.append(c);
            else {
                unsigned idx = c2-'0';
                if (idx==0) 
                    out.append(src);
                else if ((idx<=9)&&(idx<=wild.ordinality()))
                    out.append(wildlen.item(idx-1),src+wild.item(idx-1));
                else 
                    out.append(c).append(c2);
            }
        }
    }
    return true;
}

bool jlib_decl SoundexMatch(const char *src, const char *pat)
{
    char s1[5];
    char s2[5];
    return memcmp(SoundexCode(src,(size32_t)strlen(src),s1),SoundexCode(pat,(size32_t)strlen(pat),s2),4)==0;
}

//---------------------------------------------------------------------------

StringMatcher::StringMatcher()
{
    for (unsigned idx=0; idx < 256; idx++)
    {
        firstLevel[idx].value = 0;
        firstLevel[idx].table = NULL;
    }
}

void StringMatcher::freeLevel(entry * elems)
{
    for (unsigned idx = 0; idx < 256; idx++)
    {
        if (elems[idx].table)
        {
            freeLevel(elems[idx].table);
            free(elems[idx].table);
            elems[idx].table = NULL;
        }
        elems[idx].value = 0;
    }
}

StringMatcher::~StringMatcher()
{
    freeLevel(firstLevel);
}

void StringMatcher::addEntry(const char * text, unsigned action)
{
    if (!queryAddEntry((size32_t)strlen(text), text, action))
        throw MakeStringException(-1, "Duplicate entry \"%s\" added to string matcher", text);
}

void StringMatcher::addEntry(unsigned len, const char * text, unsigned action)
{
    if (!queryAddEntry(len, text, action))
        throw MakeStringException(-1, "Duplicate entry \"%*s\" added to string matcher", len, text);
}

bool StringMatcher::queryAddEntry(unsigned len, const char * text, unsigned action)
{
    if (len == 0)
        return false;

    entry * curTable = firstLevel;
    for (;;)
    {
        byte c = *text++;
        entry & curElement = curTable[c];
        if (--len == 0)
        {
            if (curElement.value != 0)
                return false;
            curElement.value = action;
            return true;
        }
        if (!curElement.table)
            curElement.table = (entry *)calloc(sizeof(entry), 256);
        curTable = curElement.table;
    }

}

unsigned StringMatcher::getMatch(unsigned maxLen, const char * text, unsigned & matchLen)
{
    unsigned bestValue = 0;
    unsigned bestLen = 0;

    if (maxLen)
    {
        const byte * start = (const byte *)text;
        const byte * cursor = (const byte *)text;
        const byte * end = (const byte *)cursor+maxLen;
        byte next = *cursor++;
        entry * cur = &firstLevel[next];
        while ((cursor != end) && cur->table)
        {
            if (cur->value)
            {
                bestValue = cur->value;
                bestLen = cursor-start;
            }

            next = *cursor++;
            cur = &cur->table[next];
        }
        if (cur->value)
        {
            matchLen = cursor-start;
            return cur->value;
        }
    }
    matchLen = bestLen;
    return bestValue;
}

void addActionList(StringMatcher & matcher, const char * text, unsigned action, unsigned * maxElementLength)
{
    if (!text)
        return;

    unsigned idx=0;
    while (*text)
    {
        StringBuffer str;
        while (*text)
        {
            char next = *text++;
            if (next == ',')
                break;
            if (next == '\\' && *text)
            {
                next = *text++;
                switch (next)
                {
                case 'r': next = '\r'; break;
                case 'n': next = '\n'; break;
                case 't': next = '\t'; break;
                case 'x':
                    //hex constant - at least we can define spaces then...
                    if (text[0] && text[1])
                    {
                        next = (hex2num(*text) << 4) | hex2num(text[1]);
                        text+=2;
                    }
                    break;
                default:
                    break; //otherwise \ just quotes the character e.g. \,
                }
            }
            str.append(next);
        }
        if (str.length())
        {
            matcher.addEntry(str.str(), action+(idx++<<8));
            if (maxElementLength && (str.length() > *maxElementLength))
                *maxElementLength = str.length();
        }
    }
}



//---------------------------------------------------------------------------

#if 0
#define MATCHsimple  0
#define MATCHwild    1
#define MATCHregular 2
#define MATCHsoundex 3
#define MATCHmask   0x0f
#define MATCHnocase 0x10


static class _MatchRE : public RegExpr
{
public:
  bool init(const char *rexpr,int nocase) {
    if (!cachere||(cachenocase!=nocase)||!strequ(cachere,rexpr)) {
      free(cachere);
      cachere = strdup(rexpr);
      cachenocase = (unsigned char)nocase;
      return RegExpr::init(cachere,cachenocase);
    }
    return true;
  }
  _MatchRE() { cachere=NULL; }
  ~_MatchRE() { free(cachere); }
private:
  char *cachere;
  unsigned char cachenocase;
} MatchRE;


extern "Pascal"
long Cla$MATCH(/* STRING s, STRING p, */ unsigned char flags)
{
  StringBuffer s;
  s.PopString();
  s.clip();
  StringBuffer p;
  p.PopString();
  p.clip();
  int nocase = flags&MATCHnocase?1:0;
  switch(flags&MATCHmask) {
  case MATCHsimple: {
      if (s.strlen()!=p.strlen()) return false;
      if (nocase)
        return striequ((char *)s,(char *)p);
      return strequ((char *)s,(char *)p);
    }
    break;
  case MATCHwild:
    return WildMatch(s,s.strlen(),p,p.strlen(),nocase);
  case MATCHregular: {
      if (!MatchRE.init(p,nocase))
        return false;
      const char *fs=MatchRE.find(s,0,RE_ALL,0);
      return (fs!=NULL);
    }
    break;
  case MATCHsoundex: {
      char s1[5];
      char s2[5];
      return memcmp(SoundexCode(s,s.strlen(),s1),SoundexCode(p,p.strlen(),s2),4)==0;
    }
    break;
  }
  return false;
}

#endif
//--------------------------------
// Testing


#ifdef _DEBUG

void tr (RegExpr &RE,const char *s,const char *p,const char *r=NULL,size32_t n=0,bool nocase=false)
{
  DBGLOG("Test '%s','%s','%s',%d,%s\r\n",s,p,r?r:"-",n,nocase?"NOCASE":"");
  char l[256];
  strcpy(l,s);
  StringBuffer ds;
  RE.init(p,nocase);
  const char * f = RE.find(l,0,RE_ALL);
  if (!f)
    DBGLOG("Not Found\r\n");
  else {
    DBGLOG("Found '%s'\r\n",RE.findstr(ds));
    for (int i = 1;i<=9;i++) {
      size32_t s = RE.findlen(i);
      if (s) {
        ds.clear();
        DBGLOG("Found Sub %d,%d,'%s'\r\n",i,s,RE.findstr(ds,i));
      }
    }
  }
  if (r) {
    RE.replace(r,sizeof(l));
    DBGLOG("Replace to '%s'\r\n",l);
  }
}

#define QBF "the quick brown fox jumped over the lazy dogs"
#define RS "Nigel Was Here"

void RE$Test()
{
  RegExpr RE;
  tr(RE,QBF,"the",RS);
  tr(RE,QBF,"quick",RS);
  tr(RE,QBF,"dogs",RS);
  tr(RE,QBF,"lazz",RS);
  tr(RE,QBF,"laz.",RS);
  tr(RE,QBF,"^laz.",RS);
  tr(RE,QBF,"laz.$",RS);
  tr(RE,QBF,"^the.",RS);
  tr(RE,QBF,"dogs^",RS);
  tr(RE,QBF,"^t.*dogs^",RS);
  tr(RE,QBF,"dog.",RS);
  tr(RE,QBF,".the",RS);
  tr(RE,QBF,".he",RS);
  tr(RE,QBF,".*",RS);
  tr(RE,QBF,"q.*",RS);
  tr(RE,QBF,"q.*z",RS);
  tr(RE,QBF,"qu.*k",RS);
  tr(RE,QBF,"d{[oa]g}s",RS);
  tr(RE,QBF,"laz[a-z]",RS);
  tr(RE,QBF,"{laz[a-z]} {dogs}",RS);
  tr(RE,QBF,"the",RS,50);
  tr(RE,QBF,"quick",RS,50);
  tr(RE,QBF,"dogs",RS,50);
  tr(RE,QBF,"lazz",RS,50);
  tr(RE,QBF,"laz.",RS,50);
  tr(RE,QBF,"^laz.",RS,50);
  tr(RE,QBF,"laz.$",RS,50);
  tr(RE,QBF,"^the.",RS,50);
  tr(RE,QBF,"dogs^",RS,50);
  tr(RE,QBF,"^t.*dogs^",RS,50);
  tr(RE,QBF,"dog.",RS,50);
  tr(RE,QBF,".the",RS,50);
  tr(RE,QBF,".he",RS,50);
  tr(RE,QBF,".*",RS,50);
  tr(RE,QBF,"q.*",RS,50);
  tr(RE,QBF,"q.*z",RS,50);
  tr(RE,QBF,"qu.*k",RS,50);
  tr(RE,QBF,"d{[oa]g}s",RS,50);
  tr(RE,QBF,"laz[a-z]",RS,50);
  tr(RE,QBF,"{laz[a-z]} {dogs}",RS,50);
  tr(RE,"INRANGE   (a,b)","[ ,]INRANGE *({.+},{.+})");
  tr(RE," inrange   (a,b)","[ ,]INRANGE *({.+},{.+})",NULL,0,true);
  tr(RE,",INRANGE(aa,bb)","[ ,]INRANGE *({.+},{.+})");
  tr(RE,",INRANGE(,bb)","[ ,]INRANGE *({.+},{.+})");
  tr(RE,"10/14/1998","^{.*}/{.*}/{.*}$");
  tr(RE,"freddy","^{|{.+}\\@}{.+}$");
  tr(RE,"myname@freddy","^{|{.+}\\@}{.+}$");
  tr(RE,"freddy","^{{.+}\\@|}{.+}$");
  tr(RE,"myname@freddy","^{{.+}\\@|}{.+}$");
  tr(RE,"freddy","^{[^@]+}@?{[^@]+}$");
  tr(RE,"myname@freddy","^{.+}@{.+}$|{.+}$");
  tr(RE,"freddy","^{|{.+}@}{[^@]+}$");
  char str[256];
  strcpy(str,"myname@freddy");
  RE.init("^{|{.+}@}{[^@]+}$");
  RE.find(str);
  StringBuffer t;
  DBGLOG("Substitute to '%s'\r\n",RE.substitute(t,"#'#3###2'#"));
  t.clear();
  RE.replace(RE.findstr(t,2),sizeof(str),3);
  t.clear();
  RE.replace(RE.findstr(t,3),sizeof(str),2);
  DBGLOG("Replace to '%s'\r\n",str);
}

void FixDate(char *buff,size32_t max)
{
  RegExpr RE("[0-9]+/[0-9]+/{[0-9]+}");
  const char *s = RE.find(buff);
  while (s) {
    StringBuffer ys;
    int y=atoi(RE.findstr(ys,1));
    if (y<1000) {
       ys.clear();
       ys.append(y+1900);
       RE.replace(ys.str(),max,1);
    }
    s = RE.findnext();
   }
  }


#endif
