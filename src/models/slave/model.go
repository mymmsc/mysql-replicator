package slave

import (
	"encoding/json"
	"github.com/siddontang/go-log/log"
	"github.com/mymmsc/mysql-replicator/src/connectors"
	"github.com/mymmsc/mysql-replicator/src/connectors/clickhouse/slave"
	slave2 "github.com/mymmsc/mysql-replicator/src/connectors/mysql/slave"
	slave3 "github.com/mymmsc/mysql-replicator/src/connectors/postgresql/slave"
	slave4 "github.com/mymmsc/mysql-replicator/src/connectors/vertica/slave"
	"github.com/mymmsc/mysql-replicator/src/constants"
	"github.com/mymmsc/mysql-replicator/src/helpers"
	"github.com/mymmsc/mysql-replicator/src/tools/exit"
	"io/ioutil"
	"os"
)

type AbstractSlave interface {
	GetInsert() helpers.Query
	GetUpdate() helpers.Query
	GetDelete(all bool) helpers.Query
	GetCommitTransaction() helpers.Query
	GetBeginTransaction() helpers.Query
	GetRollbackTransaction() helpers.Query
	Exec(helpers.Query) bool
	GetConfigStruct() interface{}
	SetConfig(interface{})
	SetParams(map[string]interface{})
	ParseKey([]interface{})
	GetFields() map[string]connectors.ConfigField
	GetTable() string
	Connection() helpers.Storage
	ParseConfig()
}

type Slave struct {
	connector AbstractSlave
	config    Config
	key       string
	table     string
	channel   chan helpers.QueryAction
}

type Config struct {
	Master ConfigMaster `json:"master"`
	Slave  interface{}  `json:"slave"`
}

type ConfigMaster struct {
	Table  string   `json:"table"`
	Fields []string `json:"fields"`
}

type Header struct {
	Timestamp uint32
	LogPos    uint32
}

var slavePool map[string]Slave

func getModel() AbstractSlave {

	switch os.Getenv("SLAVE_TYPE") {
	case "mysql":
		return &slave2.Model{}
	case "clickhouse":
		return &slave.Model{}
	case "postgresql":
		return &slave3.Model{}
	case "vertica":
		return &slave4.Model{}
	}

	return &slave2.Model{}
}

func GetSlaveByName(name string) Slave {
	if tmpSlave, ok := slavePool[name]; ok {
		return tmpSlave
	}

	exit.Fatal(constants.ErrorUndefinedSlave)

	return Slave{}
}

func MakeSlavePool() {
	slavePool = make(map[string]Slave)
	makeSlave(helpers.GetTable())
}

// make model, read config by modelName, set var model
func makeSlave(modelName string) {
	tmpSlave := Slave{}

	tmpSlave.connector = getModel()

	// parse .env config
	tmpSlave.GetConnector().ParseConfig()

	// add connector config to base config
	tmpSlave.config.Slave = tmpSlave.connector.GetConfigStruct()

	// make config
	file := helpers.ReadConfig(modelName)
	byteValue, _ := ioutil.ReadAll(file)
	err := json.Unmarshal(byteValue, &tmpSlave.config)
	defer func() {
		_ = file.Close()
	}()
	if err != nil {
		exit.Fatal(err.Error())
	}

	// set model params from config
	tmpSlave.connector.SetConfig(tmpSlave.config.Slave)

	// make channel
	tmpSlave.channel = make(chan helpers.QueryAction, helpers.GetChannelSize())
	go save(tmpSlave.channel)

	slavePool[modelName] = tmpSlave
}

func (slave Slave) GetConfig() Config {
	return slave.config
}

func (slave Slave) GetConnector() AbstractSlave {
	return slave.connector
}

func (slave Slave) ClearParams() {
	slave.connector.SetParams(map[string]interface{}{})
}

func (slave Slave) TableName() string {
	return slave.GetConnector().GetTable()
}

func (slave Slave) BeforeSave() bool {
	return true
}

func (slave Slave) GetChannelLen() int {
	return len(slave.channel)
}

func (slave Slave) Insert(header *Header) {
	slave.checkConnector()
	if slave.BeforeSave() == true {
		params := slave.connector.GetInsert()
		rollbackParams := slave.connector.GetRollbackTransaction()

		slave.channel <- helpers.QueryAction{
			Method: func() bool {
				if slave.connector.Exec(params) {
					log.Infof(constants.MessageInserted, header.Timestamp, slave.TableName(), header.LogPos)
					return true
				}

				slave.error("insert")

				return false
			},
			StopMethod: func() bool {
				if slave.connector.Exec(rollbackParams) {
					log.Infof(constants.MessageTransactionRollback, header.Timestamp, slave.TableName(), header.LogPos)
					return true
				}

				slave.error("rollback transaction")

				return false
			},
		}
	}
}

func (slave Slave) Update(header *Header) {
	slave.checkConnector()
	if slave.BeforeSave() == true {
		params := slave.connector.GetUpdate()
		rollbackParams := slave.connector.GetRollbackTransaction()

		slave.channel <- helpers.QueryAction{
			Method: func() bool {
				if slave.connector.Exec(params) {
					log.Infof(constants.MessageUpdated, header.Timestamp, slave.TableName(), header.LogPos)
					return true
				}

				slave.error("update")

				return false
			},
			StopMethod: func() bool {
				if slave.connector.Exec(rollbackParams) {
					log.Infof(constants.MessageTransactionRollback, header.Timestamp, slave.TableName(), header.LogPos)
					return true
				}

				slave.error("rollback transaction")

				return false
			},
		}
	}
}

func (slave Slave) Delete(header *Header) {
	slave.checkConnector()
	params := slave.connector.GetDelete(false)
	rollbackParams := slave.connector.GetRollbackTransaction()

	slave.channel <- helpers.QueryAction{
		Method: func() bool {
			if slave.connector.Exec(params) {
				log.Infof(constants.MessageDeleted, header.Timestamp, slave.TableName(), header.LogPos)
				return true
			}

			slave.error("delete")

			return false
		},
		StopMethod: func() bool {
			if slave.connector.Exec(rollbackParams) {
				log.Infof(constants.MessageTransactionRollback, header.Timestamp, slave.TableName(), header.LogPos)
				return true
			}

			slave.error("rollback transaction")

			return false
		},
	}
}

func (slave Slave) DeleteAll(header *Header) {
	slave.checkConnector()
	params := slave.connector.GetDelete(true)
	rollbackParams := slave.connector.GetRollbackTransaction()

	slave.channel <- helpers.QueryAction{
		Method: func() bool {
			if slave.connector.Exec(params) {
				log.Infof(constants.MessageDeletedAll, header.Timestamp, slave.TableName())
				return true
			}

			slave.error("delete")

			return false
		},
		StopMethod: func() bool {
			if slave.connector.Exec(rollbackParams) {
				log.Infof(constants.MessageTransactionRollback, header.Timestamp, slave.TableName(), header.LogPos)
				return true
			}

			slave.error("rollback transaction")

			return false
		},
	}
}

func (slave Slave) BeginTransaction(header *Header) {
	slave.checkConnector()
	params := slave.connector.GetBeginTransaction()
	rollbackParams := slave.connector.GetRollbackTransaction()

	slave.channel <- helpers.QueryAction{
		Method: func() bool {
			if slave.connector.Exec(params) {
				log.Infof(constants.MessageTransactionBegin, header.Timestamp, slave.TableName(), header.LogPos)
				return true
			}

			slave.error("begin transaction")

			return false
		},
		StopMethod: func() bool {
			if slave.connector.Exec(rollbackParams) {
				log.Infof(constants.MessageTransactionRollback, header.Timestamp, slave.TableName(), header.LogPos)
				return true
			}

			slave.error("rollback transaction")

			return false
		},
	}
}

func (slave Slave) CommitTransaction(header *Header, afterSave func()) {
	slave.checkConnector()
	params := slave.connector.GetCommitTransaction()

	method := func() bool {
		if slave.connector.Exec(params) {
			log.Infof(constants.MessageTransactionCommit, header.Timestamp, slave.TableName(), header.LogPos)
			afterSave()
			return true
		}

		slave.error("commit transaction")

		return false
	}

	slave.channel <- helpers.QueryAction{
		Method:     method,
		StopMethod: method,
	}
}

func (slave Slave) checkConnector() {
	if slave.connector == nil {
		exit.Fatal(constants.ErrorSlaveConnector)
	}
}

func (slave Slave) error(operationType string) {
	exit.Fatal(constants.ErrorSave, operationType, slave.TableName())
}
