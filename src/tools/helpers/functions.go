package helpers

import (
	"github.com/siddontang/go-log/log"
	"github.com/mymmsc/mysql-replicator/src/constants"
	"github.com/mymmsc/mysql-replicator/src/helpers"
	"github.com/mymmsc/mysql-replicator/src/models/slave"
	"github.com/mymmsc/mysql-replicator/src/models/system"
	"github.com/mymmsc/mysql-replicator/src/tools/exit"
	"time"
)

const (
	Timeout = 10
)

func GetHeader() (header slave.Header, positionSet func()) {
	t := time.Now()
	header = slave.Header{
		Timestamp: uint32(t.Unix()),
		LogPos:    Position.Pos,
	}

	// dont set position for every row. Set it for all rows once
	positionSet = func() {
		return
	}

	return header, positionSet
}

func Wait(cond func() bool) {
	for {
		// waiting until save.channel is empty
		time.Sleep(Timeout * time.Second)
		if cond() {
			break
		}
	}
}

func SetPosition() {
	dbName := helpers.GetCredentials(constants.DBSlave).(helpers.CredentialsDB).DBname
	hash := helpers.MakeHash(dbName, Table)

	err := system.SetPosition(hash, Position)
	if err != nil {
		exit.Fatal(constants.ErrorSetPosition, err.Error())
	}

	log.Infof(constants.MessagePositionUpdated, Table)
}
