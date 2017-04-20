package apiGatewayDeploy

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"strconv"
)

var _ = Describe("api", func() {

	Context("GET /deployments", func() {

		It("should get empty set if no deployments", func() {

			uri, err := url.Parse(testServer.URL)
			uri.Path = deploymentsEndpoint

			res, err := http.Get(uri.String())
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()

			Expect(res.StatusCode).Should(Equal(http.StatusOK))

			var depRes ApiDeploymentResponse
			body, err := ioutil.ReadAll(res.Body)
			Expect(err).ShouldNot(HaveOccurred())
			json.Unmarshal(body, &depRes)

			Expect(len(depRes)).To(Equal(0))
			Expect(string(body)).Should(Equal("[]"))
		})

		It("should debounce requests", func(done Done) {
			var in = make(chan interface{})
			var out = make(chan []interface{})

			go debounce(in, out, 3*time.Millisecond)

			go func() {
				defer GinkgoRecover()

				received, ok := <-out
				Expect(ok).To(BeTrue())
				Expect(len(received)).To(Equal(2))

				close(in)
				received, ok = <-out
				Expect(ok).To(BeFalse())

				close(done)
			}()

			in <- "x"
			in <- "y"
		})

		It("should get current deployments", func() {

			deploymentID := "api_get_current"
			insertTestDeployment(testServer, deploymentID)

			uri, err := url.Parse(testServer.URL)
			uri.Path = deploymentsEndpoint

			res, err := http.Get(uri.String())
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()

			Expect(res.StatusCode).Should(Equal(http.StatusOK))

			var depRes ApiDeploymentResponse
			body, err := ioutil.ReadAll(res.Body)
			Expect(err).ShouldNot(HaveOccurred())
			json.Unmarshal(body, &depRes)

			Expect(len(depRes)).To(Equal(1))

			dep := depRes[0]

			Expect(dep.ID).To(Equal(deploymentID))
			Expect(dep.ScopeId).To(Equal(deploymentID))
			Expect(dep.DisplayName).To(Equal(deploymentID))

			var config bundleConfigJson

			err = json.Unmarshal(dep.ConfigJson, &config)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(config.Name).To(Equal("/bundles/1"))

			err = json.Unmarshal(dep.BundleConfigJson, &config)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(config.Name).To(Equal("/bundles/1"))
		})

		It("should get 304 for no change", func() {

			deploymentID := "api_no_change"
			insertTestDeployment(testServer, deploymentID)

			uri, err := url.Parse(testServer.URL)
			uri.Path = deploymentsEndpoint
			res, err := http.Get(uri.String())
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()
			Expect(res.Header.Get("etag")).ShouldNot(BeEmpty())

			req, err := http.NewRequest("GET", uri.String(), nil)
			req.Header.Add("Content-Type", "application/json")
			req.Header.Add("If-None-Match", res.Header.Get("etag"))

			res, err = http.DefaultClient.Do(req)
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()
			Expect(res.StatusCode).To(Equal(http.StatusNotModified))
		})

		It("should get empty set after blocking if no deployments", func() {

			uri, err := url.Parse(testServer.URL)
			uri.Path = deploymentsEndpoint

			query := uri.Query()
			query.Add("block", "1")
			uri.RawQuery = query.Encode()

			res, err := http.Get(uri.String())
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()

			var depRes ApiDeploymentResponse
			body, err := ioutil.ReadAll(res.Body)
			Expect(err).ShouldNot(HaveOccurred())
			json.Unmarshal(body, &depRes)

			Expect(res.StatusCode).Should(Equal(http.StatusOK))
			Expect(string(body)).Should(Equal("[]"))
		})

		It("should get new deployment set after blocking", func(done Done) {

			deploymentID := "api_get_current_blocking"
			insertTestDeployment(testServer, deploymentID)
			uri, err := url.Parse(testServer.URL)
			uri.Path = deploymentsEndpoint
			res, err := http.Get(uri.String())
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()
			eTag := res.Header.Get("etag")
			Expect(eTag).ShouldNot(BeEmpty())

			deploymentID = "api_get_current_blocking2"
			go func() {
				defer GinkgoRecover()

				query := uri.Query()
				query.Add("block", "1")
				uri.RawQuery = query.Encode()
				req, err := http.NewRequest("GET", uri.String(), nil)
				req.Header.Add("Content-Type", "application/json")
				req.Header.Add("If-None-Match", eTag)

				res, err := http.DefaultClient.Do(req)
				Expect(err).ShouldNot(HaveOccurred())
				defer res.Body.Close()
				Expect(res.StatusCode).To(Equal(http.StatusOK))

				Expect(res.Header.Get("etag")).ShouldNot(BeEmpty())
				Expect(res.Header.Get("etag")).ShouldNot(Equal(eTag))

				var depRes ApiDeploymentResponse
				body, err := ioutil.ReadAll(res.Body)
				Expect(err).ShouldNot(HaveOccurred())
				json.Unmarshal(body, &depRes)

				Expect(len(depRes)).To(Equal(2))

				dep := depRes[1]

				Expect(dep.ID).To(Equal(deploymentID))
				Expect(dep.ScopeId).To(Equal(deploymentID))
				Expect(dep.DisplayName).To(Equal(deploymentID))

				close(done)
			}()

			time.Sleep(250 * time.Millisecond) // give api call above time to block
			insertTestDeployment(testServer, deploymentID)
			deploymentsChanged <- deploymentID
		})

		It("should get 304 after blocking if no new deployment", func() {

			deploymentID := "api_no_change_blocking"
			insertTestDeployment(testServer, deploymentID)
			uri, err := url.Parse(testServer.URL)
			uri.Path = deploymentsEndpoint
			res, err := http.Get(uri.String())
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()
			Expect(res.Header.Get("etag")).ShouldNot(BeEmpty())

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

	Context("PUT /deployments", func() {

		It("should return BadRequest for invalid request", func() {

			uri, err := url.Parse(testServer.URL)
			uri.Path = deploymentsEndpoint

			deploymentResult := apiDeploymentResults{
				apiDeploymentResult{},
			}
			payload, err := json.Marshal(deploymentResult)
			Expect(err).ShouldNot(HaveOccurred())

			req, err := http.NewRequest("PUT", uri.String(), bytes.NewReader(payload))
			req.Header.Add("Content-Type", "application/json")

			resp, err := http.DefaultClient.Do(req)
			defer resp.Body.Close()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(resp.StatusCode).Should(Equal(http.StatusBadRequest))
		})

		It("should ignore deployments that can't be found", func() {

			deploymentID := "api_missing_deployment"

			uri, err := url.Parse(testServer.URL)
			uri.Path = deploymentsEndpoint

			deploymentResult := apiDeploymentResults{
				apiDeploymentResult{
					ID:     deploymentID,
					Status: RESPONSE_STATUS_SUCCESS,
				},
			}
			payload, err := json.Marshal(deploymentResult)
			Expect(err).ShouldNot(HaveOccurred())

			req, err := http.NewRequest("PUT", uri.String(), bytes.NewReader(payload))
			req.Header.Add("Content-Type", "application/json")

			resp, err := http.DefaultClient.Do(req)
			defer resp.Body.Close()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(resp.StatusCode).Should(Equal(http.StatusOK))
		})

		It("should mark a deployment as successful", func() {

			db := getDB()
			deploymentID := "api_mark_deployed"
			insertTestDeployment(testServer, deploymentID)

			uri, err := url.Parse(testServer.URL)
			uri.Path = deploymentsEndpoint

			deploymentResult := apiDeploymentResults{
				apiDeploymentResult{
					ID:     deploymentID,
					Status: RESPONSE_STATUS_SUCCESS,
				},
			}
			payload, err := json.Marshal(deploymentResult)
			Expect(err).ShouldNot(HaveOccurred())

			req, err := http.NewRequest("PUT", uri.String(), bytes.NewReader(payload))
			req.Header.Add("Content-Type", "application/json")

			resp, err := http.DefaultClient.Do(req)
			defer resp.Body.Close()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(resp.StatusCode).Should(Equal(http.StatusOK))

			var deployStatus string
			err = db.QueryRow("SELECT deploy_status FROM kms_deployments WHERE id=?", deploymentID).
				Scan(&deployStatus)
			Expect(deployStatus).Should(Equal(RESPONSE_STATUS_SUCCESS))
		})

		It("should mark a deployment as failed", func() {

			db := getDB()
			deploymentID := "api_mark_failed"
			insertTestDeployment(testServer, deploymentID)

			uri, err := url.Parse(testServer.URL)
			uri.Path = deploymentsEndpoint

			deploymentResults := apiDeploymentResults{
				apiDeploymentResult{
					ID:        deploymentID,
					Status:    RESPONSE_STATUS_FAIL,
					ErrorCode: 100,
					Message:   "Some error message",
				},
			}
			payload, err := json.Marshal(deploymentResults)
			Expect(err).ShouldNot(HaveOccurred())

			req, err := http.NewRequest("PUT", uri.String(), bytes.NewReader(payload))
			req.Header.Add("Content-Type", "application/json")

			resp, err := http.DefaultClient.Do(req)
			defer resp.Body.Close()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(resp.StatusCode).Should(Equal(http.StatusOK))

			var deployStatus, deploy_error_message string
			var deploy_error_code int
			err = db.QueryRow(`
			SELECT deploy_status, deploy_error_code, deploy_error_message
			FROM kms_deployments
			WHERE id=?`, deploymentID).Scan(&deployStatus, &deploy_error_code, &deploy_error_message)
			Expect(deployStatus).Should(Equal(RESPONSE_STATUS_FAIL))
			Expect(deploy_error_code).Should(Equal(100))
			Expect(deploy_error_message).Should(Equal("Some error message"))
		})

		It("should communicate status to tracking server", func() {

			deploymentResults := apiDeploymentResults{
				apiDeploymentResult{
					ID:        "deploymentID",
					Status:    RESPONSE_STATUS_FAIL,
					ErrorCode: 100,
					Message:   "Some error message",
				},
			}

			err := transmitDeploymentResultsToServer(deploymentResults)
			Expect(err).NotTo(HaveOccurred())

			Expect(testLastTrackerVars["clusterID"]).To(Equal("CLUSTER_ID"))
			Expect(testLastTrackerVars["instanceID"]).To(Equal("INSTANCE_ID"))
			Expect(testLastTrackerBody).ToNot(BeEmpty())

			var uploaded apiDeploymentResults
			json.Unmarshal(testLastTrackerBody, &uploaded)

			Expect(uploaded).To(Equal(deploymentResults))
		})

		It("should get iso8601 time", func() {
			testTimes := []string{"", "2017-04-05 04:47:36.462 +0000 UTC", "2017-04-05 04:47:36.462 -0700 MST", "2017-04-05T04:47:36.462Z", "2017-04-05T04:47:36.462-07:00"}
			isoTime := []string{"", "2017-04-05T04:47:36.462Z", "2017-04-05T04:47:36.462-07:00", "2017-04-05T04:47:36.462Z", "2017-04-05T04:47:36.462-07:00"}
			for i, t := range testTimes {
				log.Debug("insert deployment with timestamp: " + t)
				deploymentID := "api_time_iso8601_" + strconv.Itoa(i)
				insertTimeDeployment(testServer, deploymentID, t)
			}

			uri, err := url.Parse(testServer.URL)
			uri.Path = deploymentsEndpoint

			res, err := http.Get(uri.String())
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()

			Expect(res.StatusCode).Should(Equal(http.StatusOK))

			var depRes ApiDeploymentResponse
			body, err := ioutil.ReadAll(res.Body)
			Expect(err).ShouldNot(HaveOccurred())
			json.Unmarshal(body, &depRes)

			Expect(len(depRes)).To(Equal(len(testTimes)))

			for i, dep := range depRes {
				Expect(dep.Created).To(Equal(isoTime[i]))
				Expect(dep.Updated).To(Equal(isoTime[i]))
			}
		})
	})
})

func insertTestDeployment(testServer *httptest.Server, deploymentID string) {

	uri, err := url.Parse(testServer.URL)
	Expect(err).ShouldNot(HaveOccurred())

	uri.Path = "/bundles/1"
	bundleUri := uri.String()
	bundle := bundleConfigJson{
		Name:         uri.Path,
		URI:          bundleUri,
		ChecksumType: "crc32",
	}
	bundle.Checksum = testGetChecksum(bundle.ChecksumType, bundleUri)
	bundleJson, err := json.Marshal(bundle)
	Expect(err).ShouldNot(HaveOccurred())

	tx, err := getDB().Begin()
	Expect(err).ShouldNot(HaveOccurred())

	dep := DataDeployment{
		ID:                 deploymentID,
		BundleConfigID:     deploymentID,
		ApidClusterID:      deploymentID,
		DataScopeID:        deploymentID,
		BundleConfigJSON:   string(bundleJson),
		ConfigJSON:         string(bundleJson),
		Created:            "",
		CreatedBy:          "",
		Updated:            "",
		UpdatedBy:          "",
		BundleName:         deploymentID,
		BundleURI:          bundle.URI,
		BundleChecksum:     bundle.Checksum,
		BundleChecksumType: bundle.ChecksumType,
		LocalBundleURI:     "x",
		DeployStatus:       "",
		DeployErrorCode:    0,
		DeployErrorMessage: "",
	}

	err = InsertDeployment(tx, dep)
	Expect(err).ShouldNot(HaveOccurred())

	err = tx.Commit()
	Expect(err).ShouldNot(HaveOccurred())
}

func insertTimeDeployment(testServer *httptest.Server, deploymentID string, timestamp string) {

	uri, err := url.Parse(testServer.URL)
	Expect(err).ShouldNot(HaveOccurred())

	uri.Path = "/bundles/1"
	bundleUri := uri.String()
	bundle := bundleConfigJson{
		Name:         uri.Path,
		URI:          bundleUri,
		ChecksumType: "crc32",
	}
	bundle.Checksum = testGetChecksum(bundle.ChecksumType, bundleUri)
	bundleJson, err := json.Marshal(bundle)
	Expect(err).ShouldNot(HaveOccurred())

	tx, err := getDB().Begin()
	Expect(err).ShouldNot(HaveOccurred())

	dep := DataDeployment{
		ID:                 deploymentID,
		BundleConfigID:     deploymentID,
		ApidClusterID:      deploymentID,
		DataScopeID:        deploymentID,
		BundleConfigJSON:   string(bundleJson),
		ConfigJSON:         string(bundleJson),
		Created:            timestamp,
		CreatedBy:          "",
		Updated:            timestamp,
		UpdatedBy:          "",
		BundleName:         deploymentID,
		BundleURI:          bundle.URI,
		BundleChecksum:     bundle.Checksum,
		BundleChecksumType: bundle.ChecksumType,
		LocalBundleURI:     "x",
		DeployStatus:       "",
		DeployErrorCode:    0,
		DeployErrorMessage: "",
	}

	err = InsertDeployment(tx, dep)
	Expect(err).ShouldNot(HaveOccurred())

	err = tx.Commit()
	Expect(err).ShouldNot(HaveOccurred())
}
