package apiGatewayDeploy

import (
	"bytes"
	"encoding/json"
	"fmt"
	. "github.com/30x/apidApigeeSync" // for direct access to Payload types
	"github.com/30x/keymaster/client"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"
)

const currentDeploymentPath = "/deployments/current"

var _ = Describe("api", func() {

	PIt("should deliver deployment events to long-poll waiters")

	It("should get current deployment", func() {

		deploymentID := "api_test_1"
		insertTestDeployment(testServer, deploymentID)

		var deployStatus int
		err := db.QueryRow("SELECT deploy_status from BUNDLE_INFO WHERE deployment_id = ?;",
			deploymentID).Scan(&deployStatus)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(deployStatus).Should(Equal(DEPLOYMENT_STATE_READY))

		err = db.QueryRow("SELECT deploy_status from BUNDLE_DEPLOYMENT WHERE id = ?;",
			deploymentID).Scan(&deployStatus)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(deployStatus).Should(Equal(DEPLOYMENT_STATE_READY))

		uri, err := url.Parse(testServer.URL)
		uri.Path = currentDeploymentPath

		res, err := http.Get(uri.String())
		defer res.Body.Close()
		Expect(err).ShouldNot(HaveOccurred())

		var depRes deploymentResponse
		body, err := ioutil.ReadAll(res.Body)
		Expect(err).ShouldNot(HaveOccurred())
		json.Unmarshal(body, &depRes)

		Expect(depRes.DeploymentId).Should(Equal(deploymentID))
		Expect(res.Header.Get("etag")).Should(Equal(deploymentID))
	})

	It("should mark a deployment as deployed", func() {

		deploymentID := "api_test_2"
		insertTestDeployment(testServer, deploymentID)

		uri, err := url.Parse(testServer.URL)
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
		err = db.QueryRow("SELECT deploy_status from BUNDLE_INFO WHERE deployment_id = ?;",
			deploymentID).Scan(&deployStatus)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(deployStatus).Should(Equal(DEPLOYMENT_STATE_SUCCESS))

		err = db.QueryRow("SELECT deploy_status from BUNDLE_DEPLOYMENT WHERE id = ?;",
			deploymentID).Scan(&deployStatus)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(deployStatus).Should(Equal(DEPLOYMENT_STATE_SUCCESS))
	})

	It("should mark a deployment as failed", func() {

		deploymentID := "api_test_3"
		insertTestDeployment(testServer, deploymentID)

		uri, err := url.Parse(testServer.URL)
		uri.Path = fmt.Sprintf("/deployments/%s", deploymentID)

		deploymentResult := &client.DeploymentResult{
			ID:     deploymentID,
			Status: client.StatusFail,
			Error: &client.DeploymentError{
				ErrorCode: 100,
				Reason:    "bad juju",
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
		err = db.QueryRow("SELECT deploy_status from BUNDLE_DEPLOYMENT WHERE id = ?;",
			deploymentID).Scan(&deployStatus)
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
		EntityType:       "deployment",
		Operation:        "create",
		EntityIdentifier: entityID,
		PldCont: Payload{
			CreatedAt: time.Now().Unix(),
			Manifest:  string(bundleBytes),
		},
	}

	insertDeployment(payload)
}
