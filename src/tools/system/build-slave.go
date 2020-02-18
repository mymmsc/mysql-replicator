package system

import (
	"github.com/siddontang/go-log/log"
	"github.com/spf13/cobra"
	"github.com/mymmsc/mysql-replicator/src/constants"
	"github.com/mymmsc/mysql-replicator/src/helpers"
	"github.com/mymmsc/mysql-replicator/src/models/master"
	"github.com/mymmsc/mysql-replicator/src/tools/exit"
)

var CmdBuildTable = &cobra.Command{
	Use:   "build-slave",
	Short: "Build slave table from master",
	Long:  "Build slave table from master",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		beforeExit := func() bool {
			log.Infof(constants.MessageStopTableBuild)
			return false
		}
		exit.BeforeExitPool = append(exit.BeforeExitPool, beforeExit)

		master.GetModel().BuildSlave(helpers.GetTable())
	},
}
