package apiGatewayDeploy

import (
	"github.com/30x/apid"
	"os"
	"path"
)

const (
	configBundleDirKey = "gatewaydeploy_bundle_dir"
)

var (
	services   apid.Services
	log        apid.LogService
	data       apid.DataService
	bundlePath string
)

func init() {
	apid.RegisterPlugin(initPlugin)
}

func initPlugin(s apid.Services) (apid.PluginData, error) {
	services = s
	log = services.Log().ForModule("apiGatewayDeploy")
	log.Debug("start init")

	config := services.Config()
	config.SetDefault(configBundleDirKey, "bundles")

	data = services.Data()

	relativeBundlePath := config.GetString(configBundleDirKey)
	if err := os.MkdirAll(relativeBundlePath, 0700); err != nil {
		log.Panicf("Failed bundle directory creation: %v", err)
	}
	storagePath := config.GetString("local_storage_path")
	bundlePath = path.Join(storagePath, relativeBundlePath)
	log.Infof("Bundle directory path is %s", bundlePath)

	go distributeEvents()

	initListener(services)

	log.Debug("end init")

	return pluginData, nil
}
