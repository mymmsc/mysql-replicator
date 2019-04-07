package connectors2

type ConfigSlave struct {
	Table  string        `json:"table"`
	Fields []ConfigField `json:"fields"`
}

type ConfigBeforeSave struct {
	Method string        `json:"method"`
	Params []interface{} `json:"params"`
}

type ConfigField struct {
	Name       string           `json:"name"`
	Key        bool             `json:"key"`
	Mode       string           `json:"mode"`
	BeforeSave ConfigBeforeSave `json:"beforeSave"`
}
