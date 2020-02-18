package system

import (
	"github.com/spf13/cobra"
	master2 "github.com/mymmsc/mysql-replicator/src/models/master"
)

var CmdListen = &cobra.Command{
	Use:   "listen",
	Short: "Listen master binlog",
	Long:  "Listen master binlog",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		master2.GetModel().Listen()
	},
}
