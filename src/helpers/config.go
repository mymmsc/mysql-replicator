package helpers

import (
	"github.com/joho/godotenv"
	"github.com/mymmsc/mysql-replicator/src/constants"
	"github.com/mymmsc/mysql-replicator/src/tools/exit"
	"os"
	"strconv"
)

type Credentials struct {
	Type string
}

type CredentialsDB struct {
	Credentials
	Host   string
	Port   int
	User   string
	Pass   string
	DBname string
}

type CredentialsAMQP struct {
	Credentials
	Host string
	Port int
	User string
	Pass string
}

var master CredentialsDB
var slave interface{}

var table string
var channelSize int
var slaveId int
var masterLogFilePrefix string

func MakeCredentials() {
	err := godotenv.Load()

	if err != nil {
		exit.Fatal("Error loading .env file")
	}

	masterPort, _ := strconv.Atoi(os.Getenv("MASTER_PORT"))
	master = CredentialsDB{
		Credentials{
			os.Getenv("MASTER_TYPE"),
		},
		os.Getenv("MASTER_HOST"),
		masterPort,
		os.Getenv("MASTER_USER"),
		os.Getenv("MASTER_PASS"),
		os.Getenv("MASTER_DBNAME"),
	}

	table = os.Getenv("TABLE")

	channelSize, _ = strconv.Atoi(os.Getenv("CHANNEL_SIZE"))
	slaveId, _ = strconv.Atoi(os.Getenv("SLAVE_ID"))
	masterLogFilePrefix = os.Getenv("MASTER_LOG_FILE_PREFIX")
}

func GetCredentials(storageType string) interface{} {
	switch storageType {
	case constants.DBMaster:
		return getMaster()
	case constants.DBSlave:
		return getSlave()
	default:
		return Credentials{}
	}
}

func getMaster() CredentialsDB {
	return master
}

func getSlave() interface{} {
	return slave
}

func GetTable() string {
	return table
}

func GetSlaveId() int {
	return slaveId
}

func GetChannelSize() int {
	return channelSize
}

func GetMasterLogFilePrefix() string {
	return masterLogFilePrefix
}

func ParseDBConfig() {
	slavePort, _ := strconv.Atoi(os.Getenv("SLAVE_PORT"))

	slave = CredentialsDB{
		Credentials{
			os.Getenv("SLAVE_TYPE"),
		},
		os.Getenv("SLAVE_HOST"),
		slavePort,
		os.Getenv("SLAVE_USER"),
		os.Getenv("SLAVE_PASS"),
		os.Getenv("SLAVE_DBNAME"),
	}
}

func ParseAMQPConfig() {
	slavePort, _ := strconv.Atoi(os.Getenv("SLAVE_PORT"))

	slave = CredentialsAMQP{
		Credentials{
			os.Getenv("SLAVE_TYPE"),
		},
		os.Getenv("SLAVE_HOST"),
		slavePort,
		os.Getenv("SLAVE_USER"),
		os.Getenv("SLAVE_PASS"),
	}
}
