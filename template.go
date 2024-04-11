package main

type TemplateData struct {
	Structs []Struct
	Tables  []string
}

type Struct struct {
	Table  string
	Schema []Column
}
