package apiGatewayDeploy

import (
	"os"
	"path/filepath"
	"github.com/30x/apidApigeeSync"
	"github.com/30x/apid"
	"database/sql"
	"github.com/30x/apidGatewayDeploy/github"
)

const (
	configBundleDir = "gatewaydeploy_bundle_dir"
	configGithubAccessToken = "gatewaydeploy_github_accesstoken"
)

var (
	log apid.LogService
	data apid.DataService
)

func init() {
	apid.RegisterPlugin(initPlugin)
}

func initPlugin(services apid.Services) error {
	log = services.Log().ForModule("apiGatewayDeploy")
	log.Debug("start init")

	github.Init(services)

	config := services.Config()
	config.SetDefault(configBundleDir, "/var/tmp") // todo: good default?

	api := services.API()
	events := services.Events()
	data = services.Data()

	var err error
	bundleDir := config.GetString(configBundleDir)
	if err := os.MkdirAll(bundleDir, 0700); err != nil {
		log.Panicf("Failed bundle directory creation: %v", err)
	}
	bundlePathAbs, err = filepath.Abs(bundleDir)
	if err != nil {
		log.Panicf("Cant find Abs Path : %v", err)
	}
	log.Infof("Bundle directory path is %s", bundlePathAbs)

	gitHubAccessToken = config.GetString(configGithubAccessToken)

	db, err := data.DB()
	if err != nil {
		log.Panic("Unable to access DB", err)
	}

	var count int
	row := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='bundle_deployment';")
	if err := row.Scan(&count); err != nil {
		log.Panic("Unable to setup database", err)
	}
	if count == 0 {
		createTables(db)
	}

	go distributeEvents()

	api.HandleFunc("/deployments/{depid}", respHandler)
	api.HandleFunc("/deployments/current", handleCurrentDeployment)

	events.Listen(apidApigeeSync.ApigeeSyncEventSelector, &apigeeSyncHandler{})

	orchestrateDeployment()

	log.Debug("end init")

	return nil
}

func createTables(db *sql.DB) {
	_, err := db.Exec("CREATE TABLE bundle_deployment (org varchar(255), id varchar(255), uri varchar(255), env varchar(255), etag varchar(255), manifest text, created_at integer, modified_at integer, deploy_status integer, error_code varchar(255), PRIMARY KEY (id)); CREATE TABLE bundle_info (type integer, env varchar(255), org varchar(255), id varchar(255), url varchar(255), file_url varchar(255), created_at integer, modified_at integer, deployment_id varchar(255), etag varchar(255), custom_tag varchar(255), deploy_status integer, error_code integer, error_reason text, PRIMARY KEY (id), FOREIGN KEY (deployment_id) references BUNDLE_DEPLOYMENT(id) ON DELETE CASCADE);")
	if err != nil {
		log.Panic("Unable to initialize DB", err)
	}
}
