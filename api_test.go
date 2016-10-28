package apiGatewayDeploy

import (
	"bytes"
	"encoding/json"
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"
)

var _ = Describe("api", func() {

	Context("GET /deployments/current", func() {

		It("should get 404 if no deployments", func() {

			_, err := db.Exec("DELETE FROM gateway_deploy_deployment")
			Expect(err).ShouldNot(HaveOccurred())

			uri, err := url.Parse(testServer.URL)
			uri.Path = "/deployments/current"

			res, err := http.Get(uri.String())
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()
			Expect(res.StatusCode).Should(Equal(http.StatusNotFound))
		})

		It("should get current deployment", func() {

			deploymentID := "api_get_current"
			insertTestDeployment(testServer, deploymentID)

			uri, err := url.Parse(testServer.URL)
			uri.Path = "/deployments/current"

			res, err := http.Get(uri.String())
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()

			var depRes deployment
			body, err := ioutil.ReadAll(res.Body)
			Expect(err).ShouldNot(HaveOccurred())
			json.Unmarshal(body, &depRes)

			Expect(depRes.DeploymentId).Should(Equal(deploymentID))
			Expect(res.Header.Get("etag")).Should(Equal(deploymentID))
		})

		It("should get 304 for no change", func() {

			deploymentID := "api_no_change"
			insertTestDeployment(testServer, deploymentID)

			uri, err := url.Parse(testServer.URL)
			uri.Path = "/deployments/current"
			res, err := http.Get(uri.String())
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()

			req, err := http.NewRequest("GET", uri.String(), nil)
			req.Header.Add("Content-Type", "application/json")
			req.Header.Add("If-None-Match", res.Header.Get("etag"))

			res, err = http.DefaultClient.Do(req)
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()
			Expect(res.StatusCode).To(Equal(http.StatusNotModified))
		})

		It("should get 404 after blocking if no deployment", func() {

			_, err := db.Exec("DELETE FROM gateway_deploy_deployment")
			Expect(err).ShouldNot(HaveOccurred())

			uri, err := url.Parse(testServer.URL)
			uri.Path = "/deployments/current"

			query := uri.Query()
			query.Add("block", "1")
			uri.RawQuery = query.Encode()
			res, err := http.Get(uri.String())
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()
			Expect(res.StatusCode).Should(Equal(http.StatusNotFound))
		})

		It("should get new deployment after blocking", func(done Done) {

			deploymentID := "api_get_current_blocking"
			insertTestDeployment(testServer, deploymentID)
			uri, err := url.Parse(testServer.URL)
			uri.Path = "/deployments/current"
			res, err := http.Get(uri.String())
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()

			deploymentID = "api_get_current_blocking2"
			go func() {
				query := uri.Query()
				query.Add("block", "1")
				uri.RawQuery = query.Encode()
				req, err := http.NewRequest("GET", uri.String(), nil)
				req.Header.Add("Content-Type", "application/json")
				req.Header.Add("If-None-Match", res.Header.Get("etag"))

				res, err := http.DefaultClient.Do(req)
				Expect(err).ShouldNot(HaveOccurred())
				defer res.Body.Close()
				Expect(res.StatusCode).To(Equal(http.StatusOK))

				var depRes deployment
				body, err := ioutil.ReadAll(res.Body)
				Expect(err).ShouldNot(HaveOccurred())
				json.Unmarshal(body, &depRes)

				Expect(depRes.DeploymentId).Should(Equal(deploymentID))

				close(done)
			}()

			time.Sleep(50 * time.Millisecond) // make sure API call is made and blocks
			insertTestDeployment(testServer, deploymentID)
			incoming <- deploymentID
		})

		It("should get 304 after blocking if no new deployment", func() {

			deploymentID := "api_no_change_blocking"
			insertTestDeployment(testServer, deploymentID)
			uri, err := url.Parse(testServer.URL)
			uri.Path = "/deployments/current"
			res, err := http.Get(uri.String())
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()

			query := uri.Query()
			query.Add("block", "1")
			uri.RawQuery = query.Encode()
			req, err := http.NewRequest("GET", uri.String(), nil)
			req.Header.Add("Content-Type", "application/json")
			req.Header.Add("If-None-Match", res.Header.Get("etag"))

			res, err = http.DefaultClient.Do(req)
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()
			Expect(res.StatusCode).To(Equal(http.StatusNotModified))
		})
	})

	Context("POST /deployments/{ID}", func() {

		It("should return a 404 for missing deployment", func() {

			deploymentID := "api_missing_deployment"

			uri, err := url.Parse(testServer.URL)
			uri.Path = fmt.Sprintf("/deployments/%s", deploymentID)

			deploymentResult := deploymentResponse{
				Status: RESPONSE_STATUS_SUCCESS,
			}

			payload, err := json.Marshal(deploymentResult)
			Expect(err).ShouldNot(HaveOccurred())

			req, err := http.NewRequest("POST", uri.String(), bytes.NewReader(payload))
			req.Header.Add("Content-Type", "application/json")

			resp, err := http.DefaultClient.Do(req)
			defer resp.Body.Close()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(resp.StatusCode).Should(Equal(http.StatusNotFound))
		})

		It("should mark a deployment as deployed", func() {

			deploymentID := "api_mark_deployed"
			insertTestDeployment(testServer, deploymentID)

			uri, err := url.Parse(testServer.URL)
			uri.Path = fmt.Sprintf("/deployments/%s", deploymentID)

			deploymentResult := deploymentResponse{
				Status: RESPONSE_STATUS_SUCCESS,
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
			err = db.QueryRow("SELECT status FROM gateway_deploy_deployment WHERE id=?", deploymentID).
				Scan(&deployStatus)
			Expect(deployStatus).Should(Equal(DEPLOYMENT_STATE_SUCCESS))

			rows, err := db.Query("SELECT status from gateway_deploy_bundle WHERE id = ?;", deploymentID)
			Expect(err).ShouldNot(HaveOccurred())
			for rows.Next() {
				rows.Scan(&deployStatus)
				Expect(deployStatus).Should(Equal(DEPLOYMENT_STATE_SUCCESS))
			}
		})

		It("should mark a deployment as failed", func() {

			deploymentID := "api_test_3"
			insertTestDeployment(testServer, deploymentID)

			uri, err := url.Parse(testServer.URL)
			uri.Path = fmt.Sprintf("/deployments/%s", deploymentID)

			deploymentResult := deploymentResponse{
				Status: RESPONSE_STATUS_FAIL,
				GWbunRsp: deploymentErrorResponse{
					ErrorCode: 100,
					Reason: "bad juju",
					//ErrorDetails: []deploymentErrorDetail{ // todo: add tests for bundle errors
					//	{
					//		BundleId: "",
					//		ErrorCode: 100,
					//		Reason: "Zombies",
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
			err = db.QueryRow("SELECT status from gateway_deploy_deployment WHERE id = ?;",
				deploymentID).Scan(&deployStatus)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(deployStatus).Should(Equal(DEPLOYMENT_STATE_ERR_GWY))
		})
	})
})

func insertTestDeployment(server *httptest.Server, depID string) {

	uri, err := url.Parse(server.URL)
	Expect(err).ShouldNot(HaveOccurred())
	uri.Path = "/bundle"
	bundleUri := uri.String()

	manifest := bundleManifest{
		systemBundle{
			bundleUri,
		},
		[]dependantBundle{
			{
				bundleUri,
				"some-scope",
			},
		},
	}

	err = insertDeployment(depID, manifest)
	Expect(err).ShouldNot(HaveOccurred())

	err = updateDeploymentStatus(db, depID, DEPLOYMENT_STATE_READY, 0)
	Expect(err).ShouldNot(HaveOccurred())
}
