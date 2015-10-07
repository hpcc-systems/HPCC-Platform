import QtQuick 2.4
import QtQuick.Controls 1.3

Item {
    id: gRoot
    width: 900
    height: 700
    property alias root: gRoot
    ScrollView{
        id: scrollRoot
        anchors.fill: parent
        ListView{
            id: listRoot
            property int nest: 0
            property int visibility: 0
            anchors.fill: parent
            spacing: 4
            model: accordionModel
            opacity: !visibility
            transitions: Transition {
                NumberAnimation { properties: "opacity"; easing.type: Easing.InOutQuad; duration: 1000  }
            }
        }
    }
    VisualItemModel {
        id: accordionModel
        AccordionItem{
            title: "1"
            contentModel: VisualItemModel {
                Table{
                    tableContent: ListModel {
                        ListElement {
                            value: ListElement {value: [ListElement{value: "A Masterpiece"}] type: "field"; tooltip:"Hey" }
                            key: ListElement {value: [ListElement{value: "Gabriel"}]}
                            alphabet: ListElement { value: [ListElement{value: "soup"}] type: "field" }
                        }
                        ListElement {
                            value: ListElement {value: [ListElement{value: "Brilliance"}] type: "field"; placeholder: "lel"}
                            key: ListElement { value: [ListElement{value: "Jens"}] type: "field"}
                            alphabet: ListElement { value: [ListElement{value: "order"}] }
                        }
                        ListElement {
                            value: ListElement {value: [ListElement{value: "Outstanding"}] type: "field"}
                            key: ListElement { value: [ListElement{value: "Frederik"},ListElement{value: "Jacob"},ListElement{value: "Markus"}] type: "combo" }
                            alphabet: ListElement { value: [ListElement{value: "yodels"}] }
                        }
                    }
                    columnNames: [{role:"key",title:"key"}, {role:"value",title:"value"},{role:"alphabet",title:"Alpha"}]
                }
                Table{
                    tableContent: ListModel {
                        ListElement {
                            value: ListElement {value: [ListElement{value: "A Masterpiece"}] type: "field"; tooltip:"Hey" }
                            key: ListElement {value: [ListElement{value: "Gabriel"}]}
                        }
                        ListElement {
                            value: ListElement {value: [ListElement{value: "Brilliance"}] type: "field"; placeholder: "lel"}
                            key: ListElement { value: [ListElement{value: "Jens"}] type: "field"}
                        }
                        ListElement {
                            value: ListElement {value: [ListElement{value: "Outstanding"}] type: "field"}
                            key: ListElement { value: [ListElement{value: "Frederik"},ListElement{value: "Jacob"},ListElement{value: "Markus"}] type: "combo" }
                        }
                    }
                    columnNames: [{role:"key",title:"key"}, {role:"value",title:"value"}]
                }
                AccordionItem{
                    title: "2"
                    contentModel: VisualItemModel {
                        Text {text: "Hello"}
                        AccordionItem{
                            title: "3"
                            contentModel: VisualItemModel {
                                AccordionItem{
                                    title: "4"
                                    contentModel: VisualItemModel {
                                        Text {text: "Hello"}
                                        AccordionItem{
                                            title: "5"
                                            contentModel: VisualItemModel {
                                                Text {text: "Hello"}
                                                AccordionItem{
                                                    title: "6"
                                                    contentModel: VisualItemModel {
                                                        Table{
                                                            tableContent: ListModel {
                                                                ListElement {
                                                                    value: ListElement {value: [ListElement{value: "A Masterpiece"}] type: "field"; tooltip:"Hey" }
                                                                    key: ListElement {value: [ListElement{value: "Gabriel"}]}
                                                                }
                                                                ListElement {
                                                                    value: ListElement {value: [ListElement{value: "Brilliance"}] type: "field"; placeholder: "lel"}
                                                                    key: ListElement { value: [ListElement{value: "Jens"}] type: "field"}
                                                                }
                                                                ListElement {
                                                                    value: ListElement {value: [ListElement{value: "Outstanding"}] type: "field"}
                                                                    key: ListElement { value: [ListElement{value: "Frederik"},ListElement{value: "Jacob"},ListElement{value: "Markus"}] type: "combo" }
                                                                }
                                                            }
                                                            columnNames: [{role:"key",title:"key"}, {role:"value",title:"value"}]
                                                        }
                                                        Text {text: "Hello"}
                                                    }
                                                }
                                            }
                                        }
                                        Text {text: "World"}
                                    }
                                }
                            }
                        }
                    }
                }
                Text {text: "World"}
            }
        }
        AccordionItem{
            title: "7"
            contentModel: VisualItemModel {
                Table{
                    tableContent: ListModel {
                        ListElement {
                            value: ListElement {value: [ListElement{value: "A Masterpiece"}] type: "field"; tooltip:"Hey" }
                            key: ListElement {value: [ListElement{value: "Gabriel"}]}
                        }
                        ListElement {
                            value: ListElement {value: [ListElement{value: "Brilliance"}] type: "field"; placeholder: "lel"}
                            key: ListElement { value: [ListElement{value: "Jens"}] type: "field"}
                        }
                        ListElement {
                            value: ListElement {value: [ListElement{value: "Outstanding"}] type: "field"}
                            key: ListElement { value: [ListElement{value: "Frederik"},ListElement{value: "Jacob"},ListElement{value: "Markus"}] type: "combo" }
                        }
                    }
                    columnNames: [{role:"key",title:"key"}, {role:"value",title:"value"}]
                }
                Text {text: "Hello"}

            }
        }
        AccordionItem{
            title: "8"
            contentModel: VisualItemModel {
                Text {text: "Hello"}
                AccordionItem{
                    title: "9"
                    contentModel: VisualItemModel {
                        Text {text: "Hello"}
                        Text {text: "World"}
                    }
                }
                Text {text: "World"}
            }
        }
        AccordionItem{
            title: "10"
            contentModel: VisualItemModel {
                Text {text: "Hello"}
                AccordionItem{
                    title: "11"
                    contentModel: VisualItemModel {
                        Text {text: "Hello"}
                        AccordionItem{
                            title: "12"
                            contentModel: VisualItemModel {
                                /*Table{
                                    tableContent: ListModel {
                                        ListElement {
                                            value: ListElement {value: "A Masterpiece"; type: "field"; tooltip:"Hey" }
                                            key: ListElement {value: "Gabriel"}
                                            alphabet: ListElement { value: "soup" }
                                        }
                                        ListElement {
                                            value: ListElement {value: "Brilliance"; type: "field"; placeholder: "lel"}
                                            key: ListElement { value: "Jens" }
                                            alphabet: ListElement { value: "order" }
                                        }
                                        ListElement {
                                            value: ListElement {value: "Outstanding"; type: "field"}
                                            key: ListElement { value: "Frederik" }
                                            alphabet: ListElement { value: "yodels" }
                                        }
                                    }
                                    columnNames: [{role:"key",title:"key"}, {role:"value",title:"value"},{role:"alphabet",title:"Alpha"}]
                                }*/
                                Text {text: "Hello"}
                            }
                        }
                    }
                }
                Text {text: "World"}
            }
        }
    }
}		