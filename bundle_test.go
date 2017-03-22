package apiGatewayDeploy

import (
	"encoding/json"
	"net/url"
	"time"

	"net/http"
	"net/http/httptest"

	"io/ioutil"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("bundle", func() {

	Context("download", func() {

		It("should timeout connection and retry", func() {
			defer func() {
				bundleDownloadConnTimeout = time.Second
			}()
			bundleDownloadConnTimeout = 100 * time.Millisecond
			firstTime := true
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if firstTime {
					firstTime = false
					time.Sleep(1 * time.Second)
					w.WriteHeader(500)
				} else {
					w.Write([]byte("/bundles/longfail"))
				}
			}))
			defer ts.Close()

			uri, err := url.Parse(ts.URL)
			Expect(err).ShouldNot(HaveOccurred())
			uri.Path = "/bundles/longfail"

			tx, err := getDB().Begin()
			Expect(err).ShouldNot(HaveOccurred())

			deploymentID := "bundle_download_fail"
			dep := DataDeployment{
				ID:                 deploymentID,
				DataScopeID:        deploymentID,
				BundleURI:          uri.String(),
				BundleChecksum:     testGetChecksum("crc32", uri.String()),
				BundleChecksumType: "crc32",
			}

			err = InsertDeployment(tx, dep)
			Expect(err).ShouldNot(HaveOccurred())

			err = tx.Commit()
			Expect(err).ShouldNot(HaveOccurred())

			queueDownloadRequest(dep)

			var listener = make(chan deploymentsResult)
			addSubscriber <- listener
			result := <-listener

			Expect(result.err).NotTo(HaveOccurred())
			Expect(len(result.deployments)).To(Equal(1))
			d := result.deployments[0]
			Expect(d.ID).To(Equal(deploymentID))
		})

		It("should timeout deployment, mark status as failed, then finish", func() {

			proceed := make(chan bool)
			failedOnce := false
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if failedOnce {
					proceed <- true
					time.Sleep(markDeploymentFailedAfter)
					w.Write([]byte("/bundles/longfail"))
				} else {
					failedOnce = true
					time.Sleep(markDeploymentFailedAfter)
					w.WriteHeader(500)
				}
			}))
			defer ts.Close()

			deploymentID := "bundle_download_fail"

			uri, err := url.Parse(ts.URL)
			Expect(err).ShouldNot(HaveOccurred())

			uri.Path = "/bundles/longfail"
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
				LocalBundleURI:     "",
				DeployStatus:       "",
				DeployErrorCode:    0,
				DeployErrorMessage: "",
			}

			err = InsertDeployment(tx, dep)
			Expect(err).ShouldNot(HaveOccurred())

			err = tx.Commit()
			Expect(err).ShouldNot(HaveOccurred())

			trackerHit := false
			tracker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer GinkgoRecover()

				b, err := ioutil.ReadAll(r.Body)
				Expect(err).ShouldNot(HaveOccurred())
				var received apiDeploymentResults
				json.Unmarshal(b, &received)

				expected := apiDeploymentResults{
					{
						ID:        deploymentID,
						Status:    RESPONSE_STATUS_FAIL,
						ErrorCode: TRACKER_ERR_BUNDLE_DOWNLOAD_TIMEOUT,
						Message:   "bundle download failed",
					},
				}
				Expect(received).To(Equal(expected))
				trackerHit = true
				w.Write([]byte("OK"))
			}))
			defer tracker.Close()
			apiServerBaseURI, err = url.Parse(tracker.URL)
			Expect(err).ShouldNot(HaveOccurred())

			queueDownloadRequest(dep)

			<-proceed

			// get error state deployment
			deployments, err := getDeployments("WHERE id=$1", deploymentID)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(len(deployments)).To(Equal(1))
			d := deployments[0]

			Expect(d.ID).To(Equal(deploymentID))
			Expect(d.DeployStatus).To(Equal(RESPONSE_STATUS_FAIL))
			Expect(d.DeployErrorCode).To(Equal(TRACKER_ERR_BUNDLE_DOWNLOAD_TIMEOUT))
			Expect(d.DeployErrorMessage).ToNot(BeEmpty())
			Expect(d.LocalBundleURI).To(BeEmpty())

			Expect(trackerHit).To(BeTrue())

			var listener = make(chan deploymentsResult)
			addSubscriber <- listener
			<-listener

			// get finished deployment
			// still in error state (let client update), but with valid local bundle
			deployments, err = getDeployments("WHERE id=$1", deploymentID)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(len(deployments)).To(Equal(1))
			d = deployments[0]

			Expect(d.ID).To(Equal(deploymentID))
			Expect(d.DeployStatus).To(Equal(RESPONSE_STATUS_FAIL))
			Expect(d.DeployErrorCode).To(Equal(TRACKER_ERR_BUNDLE_DOWNLOAD_TIMEOUT))
			Expect(d.DeployErrorMessage).ToNot(BeEmpty())
			Expect(d.LocalBundleURI).To(BeAnExistingFile())
		})

		It("should not continue attempts if deployment has been deleted", func() {

			deploymentID := "bundle_download_deployment_deleted"

			uri, err := url.Parse(testServer.URL)
			Expect(err).ShouldNot(HaveOccurred())

			uri.Path = "/bundles/failonce"
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
				LocalBundleURI:     "",
				DeployStatus:       "",
				DeployErrorCode:    0,
				DeployErrorMessage: "",
			}

			err = InsertDeployment(tx, dep)
			Expect(err).ShouldNot(HaveOccurred())

			err = tx.Commit()
			Expect(err).ShouldNot(HaveOccurred())

			queueDownloadRequest(dep)

			// skip first try
			time.Sleep(bundleRetryDelay)

			// delete deployment
			tx, err = getDB().Begin()
			Expect(err).ShouldNot(HaveOccurred())
			deleteDeployment(tx, dep.ID)
			err = tx.Commit()
			Expect(err).ShouldNot(HaveOccurred())

			// wait for final
			time.Sleep(bundleRetryDelay)

			// No way to test this programmatically currently
			// search logs for "never mind, deployment bundle_download_deployment_deleted was deleted"
		})
	})

	Context("checksums", func() {

		It("should download with empty checksumType", func() {
			checksumDownloadValid("")
		})

		It("should fail download with bad empty checksumType", func() {
			checksumDownloadInvalid("")
		})

		It("should fail download with bad md5", func() {
			checksumDownloadInvalid("md5")
		})

		It("should download with good md5", func() {
			checksumDownloadValid("md5")
		})

		It("should fail download with bad md5", func() {
			checksumDownloadInvalid("md5")
		})

		It("should download with good crc32", func() {
			checksumDownloadValid("crc32")
		})

		It("should fail download with bad crc32", func() {
			checksumDownloadInvalid("crc32")
		})

		It("should download with good sha256", func() {
			checksumDownloadValid("sha256")
		})

		It("should fail download with bad sha256", func() {
			checksumDownloadInvalid("sha256")
		})

		It("should download correctly with sha512", func() {
			checksumDownloadValid("sha512")
		})

		It("should fail download with bad sha512", func() {
			checksumDownloadInvalid("sha512")
		})
	})
})

func checksumDownloadValid(checksumType string) {
	defer GinkgoRecover()

	uri, err := url.Parse(testServer.URL)
	Expect(err).ShouldNot(HaveOccurred())
	uri.Path = "/bundles/checksum"
	checksum := testGetChecksum(checksumType, uri.String())
	hash, err := getHashWriter(checksumType)
	Expect(err).NotTo(HaveOccurred())
	_, err = downloadFromURI(uri.String(), hash, checksum)
	Expect(err).NotTo(HaveOccurred())
}

func checksumDownloadInvalid(checksumType string) {
	defer GinkgoRecover()

	uri, err := url.Parse(testServer.URL)
	Expect(err).ShouldNot(HaveOccurred())
	uri.Path = "/bundles/checksum"
	checksum := "invalidchecksum"
	hash, err := getHashWriter(checksumType)
	Expect(err).NotTo(HaveOccurred())
	_, err = downloadFromURI(uri.String(), hash, checksum)
	Expect(err).To(HaveOccurred())
}
