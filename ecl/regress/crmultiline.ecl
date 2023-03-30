
x := '''\
   one
   two
   three''';

y := u'''\
   one
   two
   three''';

output(length('''
'''));
output(length(u'''
'''));
output(length('''\
'''));
output(length(u'''\
'''));
output('!'+x+'!');
output(u'!'+y+u'!');
output((DATA)('!'+x+'!'));
output((DATA)(u'!'+y+u'!'));
