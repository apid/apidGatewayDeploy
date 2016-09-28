package apiGatewayDeploy

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/30x/apidApigeeSync" // for direct access to Payload types
	"database/sql"
	"net/http/httptest"
	"github.com/30x/apid"
	"github.com/30x/apid/factory"
	"io/ioutil"
	"net/http"
	"net/url"
	"encoding/json"
	"time"
	"os"
	"fmt"
	"bytes"
	"github.com/30x/keymaster/client"
)

const (
	currentDeploymentPath = "/deployments/current"
)

var _ = Describe("api", func() {

	var tmpDir string
	var db *sql.DB
	var server *httptest.Server

	BeforeSuite(func() {
		apid.Initialize(factory.DefaultServicesFactory())

		config := apid.Config()

		var err error
		tmpDir, err = ioutil.TempDir("", "api_test")
		Expect(err).NotTo(HaveOccurred())

		config.Set("data_path", tmpDir)
		config.Set(configBundleDir, tmpDir)

		// init() will create the tables
		apid.InitializePlugins()

		router := apid.API().Router()
		// fake bundle repo
		router.HandleFunc("/bundle", func(w http.ResponseWriter, req *http.Request) {
			w.Write([]byte("bundle stuff"))
		})
		server = httptest.NewServer(router)

		db, err = apid.Data().DB()
		Expect(err).NotTo(HaveOccurred())
	})

	AfterSuite(func() {
		apid.Events().Close()
		if (server != nil) {
			server.Close()
		}
		os.RemoveAll(tmpDir)
	})

	It("should get current deployment", func() {

		var (
			deployStatus int
			err error
			deploymentID = "api_test_1"
		)

		insertTestDeployment(server, deploymentID)

		err = db.QueryRow("SELECT deploy_status from BUNDLE_INFO WHERE deployment_id = ?;", deploymentID).Scan(&deployStatus)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(deployStatus).Should(Equal(DEPLOYMENT_STATE_READY))

		err = db.QueryRow("SELECT deploy_status from BUNDLE_DEPLOYMENT WHERE id = ?;", deploymentID).Scan(&deployStatus)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(deployStatus).Should(Equal(DEPLOYMENT_STATE_READY))

		uri, err := url.Parse(server.URL)
		uri.Path = currentDeploymentPath

		res, err := http.Get(uri.String())
		defer res.Body.Close()
		Expect(err).ShouldNot(HaveOccurred())

		var depRes deploymentResponse
		body, err := ioutil.ReadAll(res.Body)
		Expect(err).ShouldNot(HaveOccurred())
		json.Unmarshal(body, &depRes)
		Expect(depRes.DeploymentId).Should(Equal(deploymentID))
	})

	It("should mark a deployment as deployed", func() {

		deploymentID := "api_test_2"
		insertTestDeployment(server, deploymentID)

		uri, err := url.Parse(server.URL)
		uri.Path = fmt.Sprintf("/deployments/%s", deploymentID)

		deploymentResult := &client.DeploymentResult{
			ID:     deploymentID,
			Status: client.StatusSuccess,
		}

		payload, err := json.Marshal(deploymentResult)
		Expect(err).ShouldNot(HaveOccurred())

		req, err := http.NewRequest("POST", uri.String(), bytes.NewReader(payload))
		req.Header.Add("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		defer resp.Body.Close()
		Expect(err).ShouldNot(HaveOccurred())
		Expect(resp.StatusCode).Should(Equal(http.StatusOK))

		var deployStatus int
		err = db.QueryRow("SELECT deploy_status from BUNDLE_INFO WHERE deployment_id = ?;", deploymentID).Scan(&deployStatus)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(deployStatus).Should(Equal(DEPLOYMENT_STATE_SUCCESS))

		err = db.QueryRow("SELECT deploy_status from BUNDLE_DEPLOYMENT WHERE id = ?;", deploymentID).Scan(&deployStatus)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(deployStatus).Should(Equal(DEPLOYMENT_STATE_SUCCESS))
	})

	It("should mark a deployment as failed", func() {

		deploymentID := "api_test_3"
		insertTestDeployment(server, deploymentID)

		uri, err := url.Parse(server.URL)
		uri.Path = fmt.Sprintf("/deployments/%s", deploymentID)

		deploymentResult := &client.DeploymentResult{
			ID:     deploymentID,
			Status: client.StatusFail,
			Error: &client.DeploymentError{
				ErrorCode: 100,
				Reason: "bad juju",
				//BundleErrors: []client.BundleError{ // todo: add tests for bundle errors
				//	{
				//		BundleID: "",
				//		ErrorCode: 100,
				//		Reason: "zombies",
				//	},
				//},
			},
		}

		payload, err := json.Marshal(deploymentResult)
		Expect(err).ShouldNot(HaveOccurred())

		req, err := http.NewRequest("POST", uri.String(), bytes.NewReader(payload))
		req.Header.Add("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		defer resp.Body.Close()
		Expect(err).ShouldNot(HaveOccurred())
		Expect(resp.StatusCode).Should(Equal(http.StatusOK))

		var deployStatus int
		err = db.QueryRow("SELECT deploy_status from BUNDLE_DEPLOYMENT WHERE id = ?;", deploymentID).Scan(&deployStatus)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(deployStatus).Should(Equal(DEPLOYMENT_STATE_ERR_GWY))
	})

})

func insertTestDeployment(server *httptest.Server, entityID string) {

	uri, err := url.Parse(server.URL)
	Expect(err).ShouldNot(HaveOccurred())
	uri.Path = "/bundle"
	bundleUri := uri.String()

	bundle := bundleManifest{
		systemBundle{
			bundleUri,
		},
		[]dependantBundle{
			{
				bundleUri,
				"someorg",
				"someenv",
			},
		},
	}
	bundleBytes, err := json.Marshal(bundle)
	Expect(err).ShouldNot(HaveOccurred())

	payload := DataPayload{
		EntityType: "deployment",
		Operation: "create",
		EntityIdentifier: entityID,
		PldCont: Payload{
			CreatedAt: time.Now().Unix(),
			Manifest: string(bundleBytes),
		},
	}

	insertDeployment(payload)
}