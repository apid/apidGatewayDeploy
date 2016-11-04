package main

import (
	"flag"
	"github.com/30x/apid"
	"github.com/30x/apid/factory"
	_ "github.com/30x/apidGatewayDeploy"
	"io/ioutil"
	"github.com/30x/transicator/common"
	"github.com/30x/apidGatewayDeploy"
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

	row := common.Row{}
	row["id"] = &common.ColumnVal{Value: "deploymentID"}
	row["body"] = &common.ColumnVal{Value: string(manifest)}

	var event = common.Snapshot{}
	event.Tables = []common.Table{
		{
			Name: apiGatewayDeploy.MANIFEST_TABLE,
			Rows: []common.Row{row},
		},
	}

	apid.Events().Emit(apiGatewayDeploy.APIGEE_SYNC_EVENT, &event)
}
