//class=version
//version passedName='rectangle',rectangle.width=40
//version passedName='rectangle',rectangle.height=40
//version passedName='rectangle',rectangle.width=20,rectangle.height=20

import ^ as root;

width := #IFDEFINED(root.rectangle.width, 10);
height := #IFDEFINED(root.rectangle.height, 10);
name := #IFDEFINED(root.passedName, 'unknown');

output('The area of the ' + name + ' is ' + (string)(width * height) + '.');

