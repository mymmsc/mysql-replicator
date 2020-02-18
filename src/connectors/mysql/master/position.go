package master

import (
	"github.com/siddontang/go-mysql/mysql"
	"github.com/mymmsc/mysql-replicator/src/constants"
	"github.com/mymmsc/mysql-replicator/src/helpers"
	"github.com/mymmsc/mysql-replicator/src/models/system"
	"github.com/mymmsc/mysql-replicator/src/tools/exit"
	"strconv"
	"strings"
)

var curPosition mysql.Position
var PrevPosition map[string]mysql.Position
var SaveLocks map[string]bool

var channel chan func()

func updatePrevPosition(c chan func()) {
	for {
		method := <-c
		method()
	}
}

func GetSavedPos(table string) mysql.Position {
	dbName := helpers.GetCredentials(constants.DBSlave).(helpers.CredentialsDB).DBname
	hash := helpers.MakeHash(dbName, table)

	position := system.GetPosition(hash)
	if position.Pos == 0 {
		exit.Fatal(constants.ErrorEmptyPosition)
	}

	return mysql.Position{
		Name: position.Name,
		Pos:  position.Pos,
	}
}

func getMinPosition(position mysql.Position) mysql.Position {
	// TODO refactor this method
	// build current position
	if curPosition.Pos == 0 {
		table := helpers.GetTable()

		savedPos := GetSavedPos(table)
		tablePosition := savedPos.Pos
		tableLogFile := savedPos.Name

		tmpLogSuffix := GetLogFileSuffix(position.Name)
		tableLogSuffix := GetLogFileSuffix(savedPos.Name)

		// if log file from storage lower than log file in master - set position from storage
		if tableLogSuffix < tmpLogSuffix {
			position.Pos = uint32(tablePosition)
			position.Name = tableLogFile
		} else {
			// if log file from storage is greater or equal log file from master - check position
			if uint32(tablePosition) < position.Pos || position.Pos == 0 {
				position.Pos = uint32(tablePosition)
				position.Name = tableLogFile
			}
		}

		curPosition = position

		PrevPosition = make(map[string]mysql.Position)
		SaveLocks = make(map[string]bool)

		channel = make(chan func())
		go updatePrevPosition(channel)

		PrevPosition[table] = curPosition
		SaveLocks[table] = true
	}

	return curPosition
}

// set position for table
func SetPosition(table string, pos mysql.Position) {
	dbName := helpers.GetCredentials(constants.DBSlave).(helpers.CredentialsDB).DBname
	hash := helpers.MakeHash(dbName, table)

	channel <- func() {
		err := system.SetPosition(hash, pos)
		if err != nil {
			exit.Fatal(constants.ErrorSetPosition, err.Error())
		}
		PrevPosition[table] = pos
	}
}

func GetLowPosition(pos1 mysql.Position, pos2 mysql.Position) mysql.Position {
	position := mysql.Position{}
	pos1Suffix := GetLogFileSuffix(pos1.Name)
	pos2Suffix := GetLogFileSuffix(pos2.Name)

	// if log file from storage lower than log file in master - set position from storage
	if pos1Suffix > pos2Suffix {
		position.Pos = uint32(pos2.Pos)
		position.Name = pos2.Name
	} else {
		// if log file from storage is greater or equal log file from master - check position
		if uint32(pos1.Pos) < pos2.Pos {
			position.Pos = uint32(pos1.Pos)
			position.Name = pos1.Name
		} else {
			position.Pos = uint32(pos2.Pos)
			position.Name = pos2.Name
		}
	}

	return position
}

func GetLogFileSuffix(name string) int {
	suff, err := strconv.Atoi(strings.Replace(name, helpers.GetMasterLogFilePrefix(), "", -1))
	if err != nil {
		exit.Fatal(constants.ErrorGetMinPosition, err)
	}

	return suff
}
