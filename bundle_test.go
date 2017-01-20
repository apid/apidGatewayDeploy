package apiGatewayDeploy

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/url"
	"encoding/json"
)

var _ = Describe("bundle", func() {

	Context("download", func() {

		It("should mark the status as failed if download fails", func() {

			deploymentID := "bundle_download_fail"

			uri, err := url.Parse(testServer.URL)
			Expect(err).ShouldNot(HaveOccurred())

			uri.Path = "/bundles/alwaysfail"
			bundleUri := uri.String()
			bundle := bundleConfigJson{
				Name: uri.Path,
				URI: bundleUri,
				ChecksumType: "crc-32",
			}
			bundle.Checksum = testGetChecksum(bundle.ChecksumType, bundleUri)
			bundleJson, err := json.Marshal(bundle)
			Expect(err).ShouldNot(HaveOccurred())

			tx, err := getDB().Begin()
			Expect(err).ShouldNot(HaveOccurred())

			dep := DataDeployment{
				ID: deploymentID,
				BundleConfigID: deploymentID,
				ApidClusterID: deploymentID,
				DataScopeID: deploymentID,
				BundleConfigJSON: string(bundleJson),
				ConfigJSON: string(bundleJson),
				Created: "",
				CreatedBy: "",
				Updated: "",
				UpdatedBy: "",
				BundleName: deploymentID,
				BundleURI: bundle.URI,
				BundleChecksum: bundle.Checksum,
				BundleChecksumType: bundle.ChecksumType,
				LocalBundleURI: "",
				DeployStatus: "",
				DeployErrorCode: 0,
				DeployErrorMessage: "",
			}

			err = InsertDeployment(tx, dep)
			Expect(err).ShouldNot(HaveOccurred())

			err = tx.Commit()
			Expect(err).ShouldNot(HaveOccurred())

			var listener = make(chan string)
			addSubscriber <- listener

			downloadBundle(dep)

			// get deployment
			deployments, err := getDeployments("WHERE id=$1", deploymentID)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(len(deployments)).To(Equal(1))
			d := deployments[0]

			Expect(d.ID).To(Equal(deploymentID))
			Expect(d.LocalBundleURI).To(BeEmpty())
			Expect(d.DeployStatus).To(Equal(RESPONSE_STATUS_FAIL))
			Expect(d.DeployErrorCode).To(Equal(ERROR_CODE_TODO))
			Expect(d.DeployErrorMessage).ToNot(BeEmpty())
		})
	})
})
