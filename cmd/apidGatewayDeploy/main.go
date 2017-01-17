package main

import (
	"flag"
	"github.com/30x/apid"
	"github.com/30x/apid/factory"
	_ "github.com/30x/apidGatewayDeploy"
	"io/ioutil"
	"github.com/30x/apidGatewayDeploy"
	"os"
	"encoding/json"
)

func main() {
	deploymentsFlag := flag.String("deployments", "", "file path to a deployments file (for testing)")
	flag.Parse()
	deploymentsFile := *deploymentsFlag

	apid.Initialize(factory.DefaultServicesFactory())

	log := apid.Log()
	log.Debug("initializing...")

	configService := apid.Config()

	var deployments apiGatewayDeploy.ApiDeploymentResponse
	if deploymentsFile != "" {
		log.Printf("Running in temp dir using deployments file: %s", deploymentsFile)
		tmpDir, err := ioutil.TempDir("", "apidGatewayDeploy")
		if err != nil {
			log.Panicf("ERROR: Unable to create temp dir", err)
		}
		defer os.RemoveAll(tmpDir)

		configService.Set("data_path", tmpDir)
		configService.Set("gatewaydeploy_bundle_dir", tmpDir) // todo: legacy?

		if deploymentsFile != "" {
			bytes, err := ioutil.ReadFile(deploymentsFile)
			if err != nil {
				log.Errorf("ERROR: Unable to read bundle config file at %s", deploymentsFile)
				return

			}

			err = json.Unmarshal(bytes, &deployments)
			if err != nil {
				log.Errorf("ERROR: Unable to parse deployments %v", err)
				return
			}
		}
	}

	apid.InitializePlugins()

	insertTestDeployments(deployments)

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

func insertTestDeployments(deployments apiGatewayDeploy.ApiDeploymentResponse) error {

	if len(deployments) == 0 {
		return nil
	}

	log := apid.Log()

	db, err := apid.Data().DB()
	if err != nil {
		return err
	}
	apiGatewayDeploy.SetDB(db)

	err = apiGatewayDeploy.InitDB(db)
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	for _, ad := range deployments {

		dep := apiGatewayDeploy.DataDeployment{
			ID:                 ad.ID,
			BundleConfigID:     ad.ID,
			ApidClusterID:      ad.ID,
			DataScopeID:        ad.ScopeId,
			BundleConfigJSON:   string(ad.BundleConfigJson),
			ConfigJSON:         string(ad.ConfigJson),
			Status:             "",
			Created:            "",
			CreatedBy:          "",
			Updated:            "",
			UpdatedBy:          "",
			BundleName:         ad.DisplayName,
			BundleURI:          ad.URI,
			BundleChecksum:     "",
			BundleChecksumType: "",
			LocalBundleURI:     ad.URI,
		}


		err = apiGatewayDeploy.InsertDeployment(tx, dep)
		if err != nil {
			log.Error("Unable to insert deployment")
			return err
		}

	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	apiGatewayDeploy.InitAPI()

	return nil
}