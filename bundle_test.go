package apiGatewayDeploy

import (
	"encoding/json"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/url"
	"time"
)

var _ = Describe("bundle", func() {

	Context("download", func() {

		It("should timeout, mark status as failed, then finish", func() {

			deploymentID := "bundle_download_fail"

			uri, err := url.Parse(testServer.URL)
			Expect(err).ShouldNot(HaveOccurred())

			uri.Path = "/bundles/longfail"
			bundleUri := uri.String()
			bundle := bundleConfigJson{
				Name:         uri.Path,
				URI:          bundleUri,
				ChecksumType: "crc-32",
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

			go downloadBundle(dep)

			// give download time to timeout
			time.Sleep(bundleDownloadTimeout + (100 * time.Millisecond))

			// get error state deployment
			deployments, err := getDeployments("WHERE id=$1", deploymentID)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(len(deployments)).To(Equal(1))
			d := deployments[0]

			Expect(d.ID).To(Equal(deploymentID))
			Expect(d.DeployStatus).To(Equal(RESPONSE_STATUS_FAIL))
			Expect(d.DeployErrorCode).To(Equal(ERROR_CODE_TODO))
			Expect(d.DeployErrorMessage).ToNot(BeEmpty())
			Expect(d.LocalBundleURI).To(BeEmpty())

			var listener = make(chan string)
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
			Expect(d.DeployErrorCode).To(Equal(ERROR_CODE_TODO))
			Expect(d.DeployErrorMessage).ToNot(BeEmpty())
			Expect(d.LocalBundleURI).To(BeAnExistingFile())
		})

		// todo: temporary - this tests that checksum is disabled until server implements (XAPID-544)
		It("should TEMPORARILY download even if empty Checksum and ChecksumType", func() {

			deploymentID := "bundle_download_temporary"

			uri, err := url.Parse(testServer.URL)
			Expect(err).ShouldNot(HaveOccurred())

			uri.Path = "/bundles/1"
			bundleUri := uri.String()
			bundle := bundleConfigJson{
				Name:         uri.Path,
				URI:          bundleUri,
				ChecksumType: "",
				Checksum: "",
			}
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

			go downloadBundle(dep)

			// give download time to finish
			time.Sleep(bundleDownloadTimeout + (100 * time.Millisecond))

			deployments, err := getReadyDeployments()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(len(deployments)).To(Equal(1))
			d := deployments[0]

			Expect(d.ID).To(Equal(deploymentID))
			Expect(d.LocalBundleURI).To(BeAnExistingFile())
		})
	})
})
