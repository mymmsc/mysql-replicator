package helpers

import (
	"fmt"
	"github.com/mymmsc/mysql-replicator/src/constants"
	"github.com/mymmsc/mysql-replicator/src/tools/exit"
	"io/ioutil"
	"os"
)

const (
	pgPath          = "../files/sql/pg_"
	pgCreateTable   = "create_log_table.sql"
	pgCreateFunc    = "create_log_func.sql"
	pgCreateTrigger = "create_log_trigger.sql"
)

func getFilePath(dbType string, queryType string) string {
	var fileName string
	switch dbType {
	default: // postgree
		fileName = pgPath + "create_log_" + queryType + ".sql"
	}

	return fileName
}

func getQuery(fileName string, params ...interface{}) string {
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		exit.Fatal(constants.ErrorNoQueryFile, fileName)
	}
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		exit.Fatal(constants.ErrorQueryFileRead, err.Error())
	}
	data := string(content[:])

	return fmt.Sprintf(data, params)
}

func GetQuery(dbType string, queryType string, params ...interface{}) string {
	fileName := getFilePath(dbType, queryType)

	return getQuery(fileName, params)
}
