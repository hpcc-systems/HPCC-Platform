import gdb
import re
import gdb.printing

class StringBufferPrinter:
    """Print a StringBuffer object."""
    def __init__(self, val):
        self.val = val
    def to_string(self):
        len = int(self.val['curLen'])
        if len:
          return self.val['buffer'].string()[0:len]
        else:
          return ""
    def display_hint(self):
        return 'string'

class StringAttrPrinter:
    """Print a StringAttr object."""
    def __init__(self, val):
        self.val = val
    def to_string(self):
        return self.val['text']
    def display_hint(self):
        return 'string'

class AtomicBoolPrinter:
    """Print an atomic bool object."""
    def __init__(self, val):
        self.val = val
    def to_string(self):
        return self.val['_M_base']['_M_i']

class AtomicScalarPrinter:
    """Print an atomic scalar object."""
    def __init__(self, val):
        self.val = val
    def to_string(self):
        return self.val['_M_i']

class AtomicVectorPrinter:
    """Print an atomic pointer object."""
    def __init__(self, val):
        self.val = val
    def to_string(self):
        return self.val['_M_b']['_M_p']

class AtomPrinter:
    def __init__(self, val):
        self.val = val
    def to_string(self):
        return self.val['key']
    def display_hint(self):
        return 'string'

class CriticalSectionPrinter:
    def __init__(self, val):
        self.val = val
    def to_string(self):
        return "CriticalSection with owner=%s, depth=%d" % (self.val['owner'], self.val['depth'])

class IInterfacePrinter:
    def __init__(self, val):
        self.val = val
    def to_string(self):
      return str(self.val.dynamic_type)

class CInterfacePrinter:
    def __init__(self, val):
        self.val = val
    def to_string(self):
      return str(self.val.dynamic_type) + " xxcount=" + str(self.val['xxcount'])

class OwnedPrinter:
    def __init__(self, val):
        self.val = val
    def to_string(self):
      return str(self.val['ptr'])

class MapStringToMyClassPrinter:
    def __init__(self, val):
        self.val = val
    def to_string(self):
      cache = int(self.val['cache'])
      table = str(self.val['table'])
      tablesize = int(self.val['tablesize'])
      tablecount = int(self.val['tablecount'])
      keysize = int(self.val['keysize'])
      ignorecase = bool(self.val['ignorecase'])
      return "cache=%u table=%s tablesize=%r tablecount=%r keysize=%r ignorecase=%r" % (cache, table, tablesize, tablecount, keysize, ignorecase)

def build_pretty_printer():
    pp = gdb.printing.RegexpCollectionPrettyPrinter(
        "HPCC-Platform/jlib")
    pp.add_printer('StringBuffer', '^[V]?StringBuffer$', StringBufferPrinter)
    pp.add_printer('StringAttr', '^StringAttr$', StringAttrPrinter)
    pp.add_printer('std::atomic<bool>', '^std::atomic<bool>$', AtomicBoolPrinter)
    pp.add_printer('std::atomic<int>', '^(RelaxedA|std::a)tomic<(unsigned int|int)>$', AtomicScalarPrinter)
    pp.add_printer('std::atomic<ptr>', '^std::atomic<.*\*>$', AtomicVectorPrinter)
    pp.add_printer('Atom', '^(LowerCase)?Atom$', AtomPrinter)
    pp.add_printer('CriticalSection', '^CriticalSection$', CriticalSectionPrinter)
    pp.add_printer('IInterfacePrinter', '^IInterface$', IInterfacePrinter)
    pp.add_printer('CInterfacePrinter', '^C(Simple)?InterfaceOf<.*>$', CInterfacePrinter)
    pp.add_printer('OwnedPrinter', '^(Shared|Owned)<.*>$', OwnedPrinter)
    pp.add_printer('MapStringToMyClassPrinter', '^MapStringToMyClass<.*>$', MapStringToMyClassPrinter)
    return pp

gdb.printing.register_pretty_printer(
    #gdb.current_objfile(),
    gdb.objfiles()[0],
    build_pretty_printer())

class StackInfo (gdb.Command):
    """ stack-info n m Shows backtrace for n frames with full local variable info for m interesting ones """

    def __init__ (self):
        super(StackInfo, self).__init__ ("stack-info", gdb.COMMAND_DATA)

    def framecount():
        n = 0
        f = gdb.newest_frame()
        while f:
            n = n + 1
            f = f.older()
        return n

    def isInterestingFrame(self):
        f = gdb.selected_frame()
        if not f:
          return False
        sal = f.find_sal()
        if sal and sal.symtab and re.match("/hpcc-dev/", sal.symtab.filename):
          return True
        return False

    def invoke (self, arg, from_tty):
        self.allInteresting = set()
        argv = gdb.string_to_argv(arg)
        count = int(argv[0])
        full = int(argv[1])
        frames = StackInfo.framecount()-1
        back = 0
        while count and frames:
          gdb.execute("up 0")   # prints current frame info
          if full and (self.isInterestingFrame()):
            gdb.execute("info locals")
            full -= 1
          gdb.execute("up-silently")
          frames -= 1
          count -= 1
          back += 1
        gdb.execute("down-silently " + str(back))

StackInfo()

class AllGlobals (gdb.Command):
    """ all-globals shows all global variables defined in any module that is part of the HPCC platform """

    def __init__ (self):
        super(AllGlobals, self).__init__ ("all-globals", gdb.COMMAND_DATA)

    def invoke (self, arg, from_tty):
        ignoreFiles = set(['system/jlib/jlzw.cpp', 'system/jlib/jlog.cpp', 'system/jlib/jencrypt.cpp', 'system/jlib/jcrc.cpp',
                           'system/security/zcrypt/aes.cpp', 'common/workunit/wuattr.cpp', 'ecl/hql/reservedwords.cpp'])
        ignoreVars = set(['statsMetaData', 'roAttributes', 'roAttributeValues', 'RandomMain'])
        ignorematch = re.compile(" StatisticsMapping ")
        varmatch = re.compile("[^a-zA-Z_0-9:]([a-zA-Z_][a-z0-9_A-Z:]*)(\\[.*])?;$")
        goodfilematch = re.compile("^File /hpcc-dev/HPCC-Platform/(.*[.]cpp):$")
        filematch = re.compile("^File (.*):$")
        infile = None
        file_written = False
        allvars = gdb.execute("info variables", False, True)
        for line in allvars.splitlines():
          m = goodfilematch.search(line)
          if m:
            infile = m.group(1)
            file_written = False
            if infile in ignoreFiles:
              infile = None
          elif filematch.search(line):
            infile = None
          elif infile:
            if (ignorematch.search(line)):
              continue
            m = varmatch.search(line)
            if m:
              varname = m.group(1)
              if varname in ignoreVars:
                continue
              sym = gdb.lookup_global_symbol(varname)
              if not sym:
                sym = gdb.lookup_static_symbol(varname)
              if sym and not sym.is_constant: 
                if not file_written:
                  gdb.write('\n' + infile + ':\n')
                  file_written = True
                gdb.write('  {} = {}\n'.format(sym.name, sym.value(gdb.newest_frame())))
              # There are some variables that gdb generates names incorrectly - e.g. if type is const char *const...
              # We don't care about them... But uncomment the next two lines if you want to see them or if other things seem to be missing
              # if not sym:
              #  gdb.write(line + ' ' + varname+' not resolved\n')
            elif line:
              pass
              # These are variables we didn't managed to parse the name of...
              # gdb.write(line+'not parsed \n')
            
AllGlobals()

#see https://sourceware.org/gdb/onlinedocs/gdb/Writing-a-Pretty_002dPrinter.html#Writing-a-Pretty_002dPrinter for more information on this
