import QtQuick 2.4
import QtQuick.Controls 1.3
import QtQml.Models 2.1

Item{
	id: tableRoot
	property var columnNames
	property alias tableContent: tableContent.sourceComponent
	width: parent ? parent.width : 500
	height: childrenRect.height
	layer.enabled: true


	Loader {
		id: tableContent
	}

	Component
	{
	    id: columnComponent
	    TableViewColumn{width: 100 }
	}
	Component {
		id: fieldCol
		TextField{
            property string origText
            text: {
                var result = value;
                if (typeof ApplicationData != 'undefined' && value){
                    result = ApplicationData.getValue(value);
                    console.log(value + "|" + result)
                    if(!result){
                        console.log("I didn't like the result: ", result)
                        result = ""
                    }
                }
                origText = result;
                return result;
            }
			placeholderText: placeholder
			onEditingFinished: {
                if(origText != text){
                        tableRoot.parent.parent.parent.edited = true;
                    if(typeof ApplicationData != 'undefined')
                    {
    					console.log("Setting text \"" + text + "\" to xpath \"" + value + "\"")
    					ApplicationData.setValue(value,text)
    				}
                }

			}
		}
	}
	Component{
		id: textCol
		Text{
			text: {
				var result = value;
				if (typeof ApplicationData != 'undefined' && value){
					result = ApplicationData.getValue(value);
					if(!result){
						console.log("I didn't like the result: ", result)
						result = ""
					}
				}
				return result;
			}
		}
	}
	Component{
		id: comboCol
		ComboBox{
			model: value
            property int origIndex: {origIndex = currentIndex}
            onActivated:{
                if(origIndex != currentIndex){
                    tableRoot.parent.parent.parent.edited = true;
                    if(typeof ApplicationData != 'undefined')
                    {
                        console.log("Setting text \"" + currentText + "\" to xpath \"" + value + "\"")
                        ApplicationData.setValue(value,currentText)
                    }
                }
            }
		}
	}
	TableView {
	    id: view
	    width: parent.width
	    model: tableContent.item
	    resources:
	    {
	        var temp = []
	        for(var i=0; i<columnNames.length; i++)
	        {
	            var column  = columnNames[i]
	            temp.push(columnComponent.createObject(view, { "role": column.role, "title": column.title, "width": Qt.binding(function(){return (parent.width/columnNames.length)-16;})}))
	        }
	        return temp
	    }
	    itemDelegate: Component{
	    	Loader {
    			property var value: {
    				var result = []
    				if(styleData.value.get(0)['type'] != null && styleData.value.get(0)['type'] == "combo"){
    					for(var i = 0; i < styleData.value.get(0)['value'].count; i++){
    						result.push(styleData.value.get(0)['value'].get(i)['value'])
    					}
    				} else {
    					result = styleData.value.get(0)['value'].get(0)['value']
    				}
    				return result
    			}
                property string placeholder: styleData.value.get(0)['placeholder'] ? styleData.value.get(0)['placeholder'] : ""
                property string tooltip: styleData.value.get(0)['tooltip'] ? styleData.value.get(0)['tooltip'] : ""
                property string viewType: styleData.value.get(0)['type'] ? styleData.value.get(0)['type'] : "text"
                sourceComponent: {
                    switch (viewType){
                        case "text":
                            return textCol
                            break;
                        case "field":
                            return fieldCol
                            break;
                        case "combo":
                        	return comboCol
                        	break;
                        default: return textCol;
                    }
                }
	    	}
	    }
	    rowDelegate: Rectangle{
	    	height: 20
	    	color: styleData.alternate ? "lightgrey" : "white"
	    }
	}
}