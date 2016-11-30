package apiGatewayDeploy

import (
	"encoding/json"
	"github.com/30x/apid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/url"
	"github.com/apigee-labs/transicator/common"
	"io/ioutil"
)

var _ = Describe("listener", func() {

	It("should process ApigeeSync snapshot event", func(done Done) {

		deploymentID := "listener_test_1"

		uri, err := url.Parse(testServer.URL)
		Expect(err).ShouldNot(HaveOccurred())
		uri.Path = "/bundle/1"
		bundleUri1 := uri.String()
		uri.Path = "/bundle/2"
		bundleUri2 := uri.String()

		dep := deployment{
			DeploymentID: deploymentID,
			System: bundle{
				URI: "whatever",
			},
			Bundles: []bundle{
				{
					BundleID: "/bundle/1",
					URI: bundleUri1,
					Scope: "some-scope",
				},
				{
					BundleID: "/bundle/2",
					URI: bundleUri2,
					Scope: "some-scope",
				},
			},
		}

		depBytes, err := json.Marshal(dep)
		Expect(err).ShouldNot(HaveOccurred())

		row := common.Row{}
		row["id"] = &common.ColumnVal{Value: deploymentID}
		row["manifest_body"] = &common.ColumnVal{Value: string(depBytes)}

		var event = common.Snapshot{
			SnapshotInfo: "test",
			Tables: []common.Table{
				{
					Name: MANIFEST_TABLE,
					Rows: []common.Row{row},
				},
			},
		}

		h := &test_handler{
			deploymentID,
			func(e apid.Event) {
				defer GinkgoRecover()

				// ignore the first event, let standard listener process it
				changeSet, ok := e.(*common.Snapshot)
				if !ok || len(changeSet.Tables) > 0 {
					return
				}

				testDeployment(dep)

				close(done)
			},
		}

		apid.Events().Listen(APIGEE_SYNC_EVENT, h)
		apid.Events().Emit(APIGEE_SYNC_EVENT, &event)              // for standard listener
		apid.Events().Emit(APIGEE_SYNC_EVENT, &common.Snapshot{SnapshotInfo: "test"}) // for test listener
 	})

	It("should process ApigeeSync change event", func(done Done) {

		deploymentID := "listener_test_2"

		uri, err := url.Parse(testServer.URL)
		Expect(err).ShouldNot(HaveOccurred())
		uri.Path = "/bundle/1"
		bundleUri := uri.String()

		dep := deployment{
			DeploymentID: deploymentID,
			System: bundle{
				URI: bundleUri,
			},
			Bundles: []bundle{
				{
					BundleID: "/bundle/1",
					URI: bundleUri,
					Scope: "some-scope",
				},
			},
		}

		depBytes, err := json.Marshal(dep)
		Expect(err).ShouldNot(HaveOccurred())

		row := common.Row{}
		row["id"] = &common.ColumnVal{Value: deploymentID}
		row["manifest_body"] = &common.ColumnVal{Value: string(depBytes)}

		var event = common.ChangeList{}
		event.Changes = []common.Change{
			{
				Operation: common.Insert,
				Table: MANIFEST_TABLE,
				NewRow: row,
			},
		}

		h := &test_handler{
			deploymentID,
			func(e apid.Event) {
				defer GinkgoRecover()

				// ignore the first event, let standard listener process it
				changeSet, ok := e.(*common.ChangeList)
				if !ok || len(changeSet.Changes) > 0 {
					return
				}

				testDeployment(dep)

				close(done)
			},
		}

		apid.Events().Listen(APIGEE_SYNC_EVENT, h)
		apid.Events().Emit(APIGEE_SYNC_EVENT, &event)               // for standard listener
		apid.Events().Emit(APIGEE_SYNC_EVENT, &common.ChangeList{}) // for test listener
	})
})

type test_handler struct {
	description string
	f           func(event apid.Event)
}

func (t *test_handler) String() string {
	return t.description
}

func (t *test_handler) Handle(event apid.Event) {
	t.f(event)
}

func testDeployment(dep deployment) {

	depID, err := getCurrentDeploymentID()
	Expect(err).ShouldNot(HaveOccurred())
	Expect(depID).Should(Equal(dep.DeploymentID))

	deployment, err := getDeployment(depID)
	Expect(deployment.Bundles).To(HaveLen(len(dep.Bundles)))

	for _, b := range dep.Bundles {
		bundleFile := getBundleFilePath(depID, b.URI)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(bundleFile).To(BeARegularFile())

		bytes, err := ioutil.ReadFile(bundleFile)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(string(bytes)).Should(Equal(b.BundleID))
	}
}
