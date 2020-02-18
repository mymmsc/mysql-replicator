package system

import (
	"github.com/siddontang/go-log/log"
	"github.com/spf13/cobra"
	"github.com/mymmsc/mysql-replicator/src/constants"
	helpers2 "github.com/mymmsc/mysql-replicator/src/helpers"
	"github.com/mymmsc/mysql-replicator/src/models/slave"
	"github.com/mymmsc/mysql-replicator/src/tools/exit"
	"github.com/mymmsc/mysql-replicator/src/tools/helpers"
)

var CmdDestroyTable = &cobra.Command{
	Use:   "destroy-slave",
	Short: "Destroy slave table from master",
	Long:  "Destroy slave table from master",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		beforeExit := func() bool {
			log.Infof(constants.MessageStopTableDestroy)
			return false
		}
		exit.BeforeExitPool = append(exit.BeforeExitPool, beforeExit)

		tableName := helpers2.GetTable()
		destroyModel(tableName)
	},
}

func destroyModel(tableName string) {
	helpers.Table = tableName
	header, _ := helpers.GetHeader()

	// delete all from table
	slave.GetSlaveByName(helpers.Table).DeleteAll(&header)

	// delete position in db
	helpers.SetPosition()

	helpers.Wait(func() bool {
		return slave.GetSlaveByName(helpers.Table).GetChannelLen() == 0
	})

	log.Infof(constants.MessageTableDestroyed, slave.GetSlaveByName(helpers.Table).TableName())
}
