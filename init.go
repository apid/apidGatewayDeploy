package apiGatewayDeploy

import (
	"github.com/30x/apid"
	"github.com/30x/apidGatewayDeploy/github"
	"os"
	"path"
)

const (
	configBundleDirKey      = "gatewaydeploy_bundle_dir"
	configGithubAccessToken = "gatewaydeploy_github_accesstoken"
)

var (
	log        apid.LogService
	db         apid.DB
	bundlePath string
)

func init() {
	apid.RegisterPlugin(initPlugin)
}

func initPlugin(services apid.Services) error {
	log = services.Log().ForModule("apiGatewayDeploy")
	log.Debug("start init")

	github.Init(services)

	config := services.Config()
	config.SetDefault(configBundleDirKey, "bundles")

	var err error
	relativeBundlePath := config.GetString(configBundleDirKey)
	if err := os.MkdirAll(relativeBundlePath, 0700); err != nil {
		log.Panicf("Failed bundle directory creation: %v", err)
	}
	storagePath := config.GetString("local_storage_path")
	bundlePath = path.Join(storagePath, relativeBundlePath)
	log.Infof("Bundle directory path is %s", bundlePath)

	gitHubAccessToken = config.GetString(configGithubAccessToken)

	db, err = services.Data().DB()
	if err != nil {
		log.Panic("Unable to access DB", err)
	}
	initDB()

	go distributeEvents()

	initAPI(services)
	initListener(services)

	log.Debug("end init")

	return nil
}
