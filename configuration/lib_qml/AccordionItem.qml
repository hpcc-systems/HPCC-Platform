import QtQuick 2.4

Rectangle {
    id: accordion
    property string title
    property string altTitle
    property bool edited: false
    property int index : -1
    property var originalParent : parent
    property var colorSchemePicker:[
		["#1be7ff","#6eeb83","#e4ff1a","#e8aa14","#ff5714"],
		["#ed6a5a","#f4f1bb","#9bc1bc","#5ca4a9","#e6ebe0"],
		["#ff4e00","#8ea604","#f5bb00","#ec9f05","#bf3100"],
		["#e3b505","#95190c","#610345","#107e7d","#044b7f"],
		["#69995d","#cbac88","#edb6a3","#d2f1e4","#f8e9e9"],
		["#69995d","#84dcc6","#cbac88","#f8e9e9","#D3BEFF"], // Runner up?
		["#41771F","#F24141","#ffe066","#247ba0","#70c1b3"], // Me Gusta
		["#41771F","#F24141","#ffe066","#247ba0","#4CB963"], // Similar to Above, imo better
		["#247ba0","#70c1b3","#b2dbbf","#f3ffbd","#ff1654"],
		["#05668d","#028090","#00a896","#02c39a","#f0f3bd"],
    ]
    property var colorScheme: colorSchemePicker[7]
    property alias contentModel: theContentModel.sourceComponent
	property int nest: parent ? parent.parent ? parent.parent.nest >= -1 ? parent.parent.nest + 1 : 1 : 1 : 1
	property int originalNest: nest
	property bool expanded: false
	layer.enabled: true
    color: {
    	return colorScheme[nest%colorScheme.length]}
	border.color: "black"
	border.width: 2
    anchors.left: parent ? parent.left : root.left
    anchors.right: parent ? parent.right : root.right
    implicitHeight: titleText.height + itemView.contentHeight + 12
    height: titleText.height + 8

    Binding on height{
    	when: expanded
    	value: titleText.height + itemView.contentHeight + 12
    }

    Behavior on height { 
    	PropertyAnimation {
	    	duration: 200
	    	easing.type: Easing.OutQuad
    	} 
    }

	state:""
	states: [
		State {
			name: "EXPANDED"
			StateChangeScript { script: {
		    		
			}}
			ParentChange{
				target: accordion
				parent: scrollRoot
			}
			PropertyChanges{
				target: accordion
				nest: originalNest
			}
			AnchorChanges{
				target: accordion
				anchors.top: parent == null ? undefined : parent.top
			}
		}
	]
	transitions: Transition {
        AnchorAnimation {
            duration: 500
            easing.type: Easing.OutQuad 
        }
        ParentAnimation {
            NumberAnimation { 
            	duration: 500
            	easing.type: Easing.OutQuad 
            }
            ScriptAction { script:{
            	if(state == "")
            		listRoot.visibility = listRoot.visibility - 1
            	else if(state == "EXPANDED")
            		listRoot.visibility = listRoot.visibility + 1
            	console.log(listRoot.visibility)
            }}
        }

    }

	Text {
    	id: titleText
    	text: (altTitle ? (typeof ApplicationData != 'undefined' ? ApplicationData.getValue(altTitle) : altTitle) + "|" : "") + title + (edited ? "*" :"")
        font.bold: edited
		anchors.horizontalCenter: parent.horizontalCenter
		anchors.top: parent.top
		anchors.topMargin: 4

	    MouseArea {
	    	width: accordion.width
	    	height: parent.height
	    	anchors.horizontalCenter: parent.horizontalCenter
	    	onClicked: {
	    		expanded = !expanded
	    	}
		}
	}
	Rectangle{
		anchors.top: titleText.top
		anchors.bottom: titleText.bottom
		anchors.left: titleText.right
		anchors.leftMargin: 100
		width: 100
		color: "blue"
		visible: originalNest != 1
		MouseArea {
	        anchors.fill: parent
	        onClicked: {
	        	switch(accordion.state){
	        		case "":
	        			accordion.state = "EXPANDED"
	        			break;
	        		case "EXPANDED":
	        			accordion.state = ""
	        			break;
	        	}
	        }
	    }
	}

	Loader {
		id: theContentModel
	}

	ListView {
		id: itemView
		model: theContentModel.item
		property int nest: accordion.nest
		visible: expanded

		spacing: 8
		anchors.left: accordion.left
		anchors.right: accordion.right
		anchors.top: titleText.bottom
		anchors.bottom: accordion.bottom
		anchors.leftMargin: 8
		anchors.rightMargin: 8
	}
}
