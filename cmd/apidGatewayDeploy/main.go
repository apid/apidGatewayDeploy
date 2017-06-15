// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"flag"
	"github.com/30x/apid-core"
	"github.com/30x/apid-core/factory"
	"github.com/30x/apidGatewayDeploy"
	_ "github.com/30x/apidGatewayDeploy"
	"io/ioutil"
	"os"
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

	apid.InitializePlugins("")

	insertTestDeployments(deployments)

	// print the base url to the console
	basePath := "/deployments"
	port := configService.GetString("api_port")
	log.Print()
	log.Printf("API is at: http://localhost:%s%s", port, basePath)
	log.Print()

	// start client API listener
	api := apid.API()
	err := api.Listen()
	if err != nil {
		log.Print(err)
	}
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
