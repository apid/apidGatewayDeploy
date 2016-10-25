package main

import (
	"flag"
	"github.com/30x/apid"
	"github.com/30x/apid/factory"
	"github.com/30x/apidApigeeSync"
	_ "github.com/30x/apidGatewayDeploy"
	"io/ioutil"
	"time"
)

func main() {
	manifestFlag := flag.String("manifest", "", "file path to a manifest yaml file")
	flag.Parse()
	manifestFile := *manifestFlag

	// initialize apid using default services
	apid.Initialize(factory.DefaultServicesFactory())

	log := apid.Log()
	log.Debug("initializing...")

	config := apid.Config()

	// todo: This will change after apidApigeeSync is fixed for scopes
	config.SetDefault("apigeesync_proxy_server_base", "X")
	config.SetDefault("apigeesync_organization", "X")
	config.SetDefault("apigeesync_consumer_key", "X")
	config.SetDefault("apigeesync_consumer_secret", "X")

	// if manifest is specified, start with only the manifest using a temp dir
	var manifest []byte
	if manifestFile != "" {
		var err error
		manifest, err = ioutil.ReadFile(manifestFile)
		if err != nil {
			log.Errorf("ERROR: Unable to read manifest at %s", manifestFile)
			return
		}

		log.Printf("Running in temp dir with manifest: %s", manifestFile)
		tmpDir, err := ioutil.TempDir("", "apidGatewayDeploy")
		if err != nil {
			log.Panicf("ERROR: Unable to create temp dir", err)
		}
		config.Set("data_path", tmpDir)
		config.Set("gatewaydeploy_bundle_dir", tmpDir)
	}

	// this will call all initialization functions on all registered plugins
	apid.InitializePlugins()

	if manifest != nil {
		insertTestRecord(manifest)
	}

	// print the base url to the console
	basePath := "/deployments"
	port := config.GetString("api_port")
	log.Print()
	log.Printf("API is at: http://localhost:%s%s", port, basePath)
	log.Print()

	// start client API listener
	api := apid.API()
	err := api.Listen() // doesn't return if no error
	log.Fatalf("Error. Is something already running on port %d? %s", port, err)
}

func insertTestRecord(manifest []byte) {

	now := time.Now().Unix()
	var event = apidApigeeSync.ChangeSet{}
	event.Changes = []apidApigeeSync.ChangePayload{
		{
			Data: apidApigeeSync.DataPayload{
				EntityType:       "deployment",
				Operation:        "create",
				EntityIdentifier: "entityID",
				PldCont: apidApigeeSync.Payload{
					CreatedAt: now,
					Manifest:  string(manifest),
				},
			},
		},
	}
	apid.Events().Emit(apidApigeeSync.ApigeeSyncEventSelector, &event)
}
