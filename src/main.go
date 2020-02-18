package main

import (
	"github.com/spf13/cobra"
	"github.com/mymmsc/mysql-replicator/src/helpers"
	"github.com/mymmsc/mysql-replicator/src/models/master"
	"github.com/mymmsc/mysql-replicator/src/models/slave"
	"github.com/mymmsc/mysql-replicator/src/tools"
	"github.com/mymmsc/mysql-replicator/src/tools/system"
)

func main() {
	helpers.MakeCredentials()
	master.MakeMaster()
	slave.MakeSlavePool()
	tools.MakeHandler()

	var rootCmd = &cobra.Command{Use: "mysql-replicator"}
	rootCmd.AddCommand(
		system.CmdListen,
		system.CmdLoad,
		system.CmdSetPosition,
		system.CmdModelCreator,
		system.CmdBuildTable,
		system.CmdDestroyTable,
	)
	_ = rootCmd.Execute()
}
