package plugins

import (
	"fmt"
	"github.com/mymmsc/mysql-replicator/src/connectors"
	"github.com/mymmsc/mysql-replicator/src/constants"
	"github.com/mymmsc/mysql-replicator/src/helpers"
	"github.com/mymmsc/mysql-replicator/src/tools/exit"
	"plugin"
)

func Handle(beforeSave connectors.ConfigBeforeSave, value interface{}) interface{} {
	mod := fmt.Sprintf(constants.PluginPath, beforeSave.Handler)
	plug, err := plugin.Open(mod)
	if err != nil {
		exit.Fatal(constants.ErrorCachePluginError, err)
	}

	symHandler, err := plug.Lookup("Handler")
	if err != nil {
		exit.Fatal(constants.ErrorCachePluginError, err)
	}

	var handler helpers.Handler
	handler, ok := symHandler.(helpers.Handler)
	if !ok {
		exit.Fatal(constants.ErrorCachePluginError, "unexpected type from module symbol")
	}

	return handler.Handle(value, beforeSave.Params)
}
