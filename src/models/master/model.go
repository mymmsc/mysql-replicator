package master

import (
	"database/sql"
	"github.com/mymmsc/mysql-replicator/src/connectors/mysql"
	mysqlMaster "github.com/mymmsc/mysql-replicator/src/connectors/mysql/master"
	"github.com/mymmsc/mysql-replicator/src/connectors/postgresql"
	postgresqlMaster "github.com/mymmsc/mysql-replicator/src/connectors/postgresql/master"
	"github.com/mymmsc/mysql-replicator/src/constants"
	"github.com/mymmsc/mysql-replicator/src/helpers"
	"os"
)

type AbstractMaster interface {
	Listen()
	BuildSlave(table string)
}

var master AbstractMaster

func GetModel() AbstractMaster {
	return master
}

func MakeMaster() {
	switch os.Getenv("MASTER_TYPE") {
	case "postgresql":
		master = &postgresqlMaster.Model{}
		helpers.ConnPool.Master = postgresql.GetConnection(helpers.ConnPool.Master, constants.DBMaster).(helpers.ConnectionMaster)
	case "mysql":
		master = &mysqlMaster.Model{}
		helpers.ConnPool.Master = mysql.GetConnection(helpers.ConnPool.Master, constants.DBMaster).(helpers.ConnectionMaster)
	}
}

func Exec(params helpers.Query) bool {
	return helpers.ConnPool.Master.Exec(params)
}

func Get(params helpers.Query) *sql.Rows {
	return helpers.ConnPool.Master.Get(params)
}
