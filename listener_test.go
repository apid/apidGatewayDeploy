package apiGatewayDeploy

import (
	"encoding/json"
	"github.com/30x/apid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/url"
	"github.com/apigee-labs/transicator/common"
)

var _ = Describe("listener", func() {

	Context("ApigeeSync snapshot event", func() {

		It("should set DB and process", func(done Done) {

			deploymentID := "listener_test_1"

			uri, err := url.Parse(testServer.URL)
			Expect(err).ShouldNot(HaveOccurred())

			uri.Path = "/bundles/1"
			bundleUri := uri.String()
			bundle1 := bundleConfigJson{
				Name: uri.Path,
				URI: bundleUri,
				ChecksumType: "crc-32",
			}
			bundle1.Checksum = testGetChecksum(bundle1.ChecksumType, bundleUri)
			bundle1Json, err := json.Marshal(bundle1)
			Expect(err).ShouldNot(HaveOccurred())

			row := common.Row{}
			row["id"] = &common.ColumnVal{Value: deploymentID}
			row["bundle_config_json"] = &common.ColumnVal{Value: string(bundle1Json)}

			var event = common.Snapshot{
				SnapshotInfo: "test",
				Tables: []common.Table{
					{
						Name: DEPLOYMENT_TABLE,
						Rows: []common.Row{row},
					},
				},
			}

			var listener = make(chan string)
			addSubscriber <- listener

			apid.Events().Emit(APIGEE_SYNC_EVENT, &event)

			id := <-listener
			Expect(id).To(Equal(deploymentID))

			deployments, err := getReadyDeployments()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(len(deployments)).To(Equal(1))
			d := deployments[0]

			Expect(d.ID).To(Equal(deploymentID))
			Expect(d.BundleName).To(Equal(bundle1.Name))
			Expect(d.BundleURI).To(Equal(bundle1.URI))

			close(done)
		})
	})

	Context("ApigeeSync change event", func() {

		It("add event should add a deployment", func(done Done) {

			deploymentID := "add_test_1"

			uri, err := url.Parse(testServer.URL)
			Expect(err).ShouldNot(HaveOccurred())

			uri.Path = "/bundles/1"
			bundleUri := uri.String()
			bundle := bundleConfigJson{
				Name: uri.Path,
				URI: bundleUri,
				ChecksumType: "crc-32",
			}
			bundle.Checksum = testGetChecksum(bundle.ChecksumType, bundleUri)
			bundle1Json, err := json.Marshal(bundle)
			Expect(err).ShouldNot(HaveOccurred())

			row := common.Row{}
			row["id"] = &common.ColumnVal{Value: deploymentID}
			row["bundle_config_json"] = &common.ColumnVal{Value: string(bundle1Json)}

			var event = common.ChangeList{
				Changes: []common.Change{
					{
						Operation: common.Insert,
						Table: DEPLOYMENT_TABLE,
						NewRow: row,
					},
				},
			}

			var listener = make(chan string)
			addSubscriber <- listener

			apid.Events().Emit(APIGEE_SYNC_EVENT, &event)

			// wait for event to propagate
			id := <-listener
			Expect(id).To(Equal(deploymentID))

			deployments, err := getReadyDeployments()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(len(deployments)).To(Equal(1))
			d := deployments[0]

			Expect(d.ID).To(Equal(deploymentID))
			Expect(d.BundleName).To(Equal(bundle.Name))
			Expect(d.BundleURI).To(Equal(bundle.URI))

			close(done)
		})

		It("delete event should delete a deployment", func(done Done) {

			deploymentID := "delete_test_1"

			tx, err := getDB().Begin()
			Expect(err).ShouldNot(HaveOccurred())
			dep := DataDeployment{
				ID: deploymentID,
				LocalBundleURI: "whatever",
			}
			err = InsertDeployment(tx, dep)
			Expect(err).ShouldNot(HaveOccurred())
			err = tx.Commit()
			Expect(err).ShouldNot(HaveOccurred())

			row := common.Row{}
			row["id"] = &common.ColumnVal{Value: deploymentID}

			var event = common.ChangeList{
				Changes: []common.Change{
					{
						Operation: common.Delete,
						Table: DEPLOYMENT_TABLE,
						OldRow: row,
					},
				},
			}

			var listener = make(chan string)
			addSubscriber <- listener

			apid.Events().Emit(APIGEE_SYNC_EVENT, &event)

			id := <-listener
			Expect(id).To(Equal(deploymentID))

			deployments, err := getReadyDeployments()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(len(deployments)).To(Equal(0))

			close(done)
		})
	})
})
