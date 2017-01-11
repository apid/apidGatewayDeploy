package main

import (
	"flag"
	"github.com/30x/apid"
	"github.com/30x/apid/factory"
	_ "github.com/30x/apidGatewayDeploy"
	"io/ioutil"
	"github.com/30x/apidGatewayDeploy"
	"os"
)

func main() {
	bundleFlag := flag.String("bundle", "", "file path to a bundle file (for testing)")
	configFlag := flag.String("config", "", "file path to a bundle config file (for testing)")
	flag.Parse()
	bundleFile := *bundleFlag
	configFile := *configFlag

	apid.Initialize(factory.DefaultServicesFactory())

	log := apid.Log()
	log.Debug("initializing...")

	configService := apid.Config()

	// if bundle is specified, start in a temp dir for testing
	var bundleConfig string
	if bundleFile != "" {
		log.Printf("Running in temp dir with bundle file: %s", bundleFile)
		tmpDir, err := ioutil.TempDir("", "apidGatewayDeploy")
		if err != nil {
			log.Panicf("ERROR: Unable to create temp dir", err)
		}
		defer os.RemoveAll(tmpDir)

		configService.Set("data_path", tmpDir)
		configService.Set("gatewaydeploy_bundle_dir", tmpDir)

		if configFile != "" {
			bundleConfigBytes, err := ioutil.ReadFile(configFile)
			if err != nil {
				log.Errorf("ERROR: Unable to read bundle config file at %s", configFile)
				return

			}
			bundleConfig = string(bundleConfigBytes)
		}
	}

	apid.InitializePlugins()

	if bundleFile != "" {
		err := insertTestDeployment(bundleFile, bundleConfig)
		if err != nil {
			log.Fatal(err)
		}
	}

	// print the base url to the console
	basePath := "/deployments"
	port := configService.GetString("api_port")
	log.Print()
	log.Printf("API is at: http://localhost:%s%s", port, basePath)
	log.Print()

	// start client API listener
	api := apid.API()
	err := api.Listen() // doesn't return if no error
	log.Fatalf("Error. Is something already running on port %d? %s", port, err)
}

func insertTestDeployment(bundleFile, bundleConfig string) error {

	deploymentID := "testDeployment"

	dep := apiGatewayDeploy.DataDeployment{
		ID: deploymentID,
		BundleConfigID: deploymentID,
		ApidClusterID: deploymentID,
		DataScopeID: deploymentID,
		BundleConfigJSON: bundleConfig,
		ConfigJSON: "",
		Status: "",
		Created: "",
		CreatedBy: "",
		Updated: "",
		UpdatedBy: "",
		BundleName: deploymentID,
		BundleURI: bundleFile,
		BundleChecksum: "",
		BundleChecksumType: "",
		LocalBundleURI: bundleFile,
	}

	log := apid.Log()

	db, err := apid.Data().DB()
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	err = apiGatewayDeploy.InsertDeployment(tx, dep)
	if err != nil {
		log.Error("Unable to insert deployment")
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}