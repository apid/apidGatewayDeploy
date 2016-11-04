package apiGatewayDeploy

import (
	"database/sql"
	"github.com/30x/apid"
	"github.com/30x/apidGatewayDeploy/github"
	"os"
	"path/filepath"
)

const (
	configBundleDir         = "gatewaydeploy_bundle_dir"
	configGithubAccessToken = "gatewaydeploy_github_accesstoken"
)

var (
	log apid.LogService
	db  *sql.DB
)

func init() {
	apid.RegisterPlugin(initPlugin)
}

func initPlugin(services apid.Services) error {
	log = services.Log().ForModule("apiGatewayDeploy")
	log.Debug("start init")

	github.Init(services)

	config := services.Config()
	config.SetDefault(configBundleDir, "/var/tmp")

	var err error
	dir := config.GetString(configBundleDir)
	if err := os.MkdirAll(dir, 0700); err != nil {
		log.Panicf("Failed bundle directory creation: %v", err)
	}
	bundlePath, err = filepath.Abs(dir)
	if err != nil {
		log.Panicf("Cant find Abs Path : %v", err)
	}
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

	// todo: in goroutine?
	serviceDeploymentQueue()

	log.Debug("end init")

	return nil
}
