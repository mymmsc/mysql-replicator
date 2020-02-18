package slave

import (
	"fmt"
	"github.com/mymmsc/mysql-replicator/src/connectors"
	"github.com/mymmsc/mysql-replicator/src/connectors/mysql"
	"github.com/mymmsc/mysql-replicator/src/constants"
	"github.com/mymmsc/mysql-replicator/src/helpers"
	"strings"
)

const (
	Type      = "mysql"
	Insert    = `INSERT INTO %s(%s) VALUES(%s);`
	Update    = `UPDATE %s SET %s WHERE %s=?;`
	Delete    = `DELETE FROM %s WHERE %s=?;`
	DeleteAll = `TRUNCATE TABLE %s`
)

type Model struct {
	table       string
	key         string
	keyPosition int
	fields      map[string]connectors.ConfigField
	params      map[string]interface{}
}

func (model *Model) ParseConfig() {
	helpers.ParseDBConfig()
}

func (model Model) GetFields() map[string]connectors.ConfigField {
	return model.fields
}

func (model *Model) ParseKey(row []interface{}) {
	// TODO в зависимости от типа поля ключа, тут могут быть разные приведения типов
	// а если будет составной ключ вообще будет тяжко
	model.params[model.key] = int(row[model.keyPosition].(int32))
}

func (model *Model) GetConfigStruct() interface{} {
	return &connectors.ConfigSlave{}
}

func (model *Model) GetTable() string {
	return model.table
}

func (model *Model) SetConfig(config interface{}) {
	model.table = config.(*connectors.ConfigSlave).Table

	model.fields = make(map[string]connectors.ConfigField)
	for pos, value := range config.(*connectors.ConfigSlave).Fields {
		if model.key == "" && value.Key == true {
			model.key = value.Name
			model.keyPosition = pos
		}

		model.fields[value.Name] = value
	}
}

func (model *Model) SetParams(params map[string]interface{}) {
	model.params = params
}

func (model *Model) GetInsert() helpers.Query {
	var params []interface{}
	var fieldNames []string
	var fieldValues []string

	for _, value := range model.fields {
		fieldNames = append(fieldNames, "`"+value.Name+"`")
		fieldValues = append(fieldValues, "?")

		params = append(params, model.params[value.Name])
	}

	query := fmt.Sprintf(Insert, model.table, strings.Join(fieldNames, ","), strings.Join(fieldValues, ","))

	return helpers.Query{
		Query:  query,
		Params: params,
	}
}

func (model *Model) GetUpdate() helpers.Query {
	var params []interface{}
	var fields []string

	for _, value := range model.fields {
		fields = append(fields, "`"+value.Name+"`"+"=?")

		params = append(params, model.params[value.Name])
	}

	// add key to params
	params = append(params, model.params[model.key])

	query := fmt.Sprintf(Update, model.table, strings.Join(fields, ","), model.key)

	return helpers.Query{
		Query:  query,
		Params: params,
	}
}

func (model *Model) GetDelete(all bool) helpers.Query {
	var params []interface{}
	var query string
	if all == true {
		query = fmt.Sprintf(DeleteAll, model.table)
	} else {
		query = fmt.Sprintf(Delete, model.table, model.key)
		params = append(params, model.params[model.key])
	}
	return helpers.Query{
		Query:  query,
		Params: params,
	}
}

func (model *Model) GetCommitTransaction() helpers.Query {

	return helpers.Query{
		Query:  "COMMIT;",
		Params: []interface{}{},
	}
}

func (model *Model) GetBeginTransaction() helpers.Query {
	return helpers.Query{
		Query:  "START TRANSACTION;",
		Params: []interface{}{},
	}
}

func (model *Model) GetRollbackTransaction() helpers.Query {
	return helpers.Query{
		Query:  "ROLLBACK;",
		Params: []interface{}{},
	}
}

func (model *Model) Exec(params helpers.Query) bool {
	return model.Connection().Exec(params)
}

func (model *Model) Connection() helpers.Storage {
	helpers.ConnPool.Slave = mysql.GetConnection(helpers.ConnPool.Slave, constants.DBSlave).(helpers.Storage)
	return helpers.ConnPool.Slave
}
