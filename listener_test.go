package apiGatewayDeploy

import (
	"encoding/json"
	"github.com/30x/apid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/url"
	"github.com/30x/transicator/common"
)

var _ = Describe("listener", func() {

	It("should process ApigeeSync snapshot event", func(done Done) {

		deploymentID := "listener_test_1"

		uri, err := url.Parse(testServer.URL)
		Expect(err).ShouldNot(HaveOccurred())
		uri.Path = "/bundle"
		bundleUri := uri.String()

		dep := deployment{
			DeploymentId: deploymentID,
			System: bundle{
				URI: bundleUri,
			},
			Bundles: []bundle{
				{
					BundleId: "bun",
					URI: bundleUri,
					Scope: "some-scope",
				},
			},
		}

		depBytes, err := json.Marshal(dep)
		Expect(err).ShouldNot(HaveOccurred())

		row := common.Row{}
		row["id"] = &common.ColumnVal{Value: deploymentID}
		row["body"] = &common.ColumnVal{Value: string(depBytes)}

		var event = common.Snapshot{}
		event.Tables = []common.Table{
			{
				Name: MANIFEST_TABLE,
				Rows: []common.Row{row},
			},
		}

		h := &test_handler{
			"checkDatabase",
			func(e apid.Event) {
				defer GinkgoRecover()

				// ignore the first event, let standard listener process it
				changeSet := e.(*common.Snapshot)
				if len(changeSet.Tables) > 0 {
					return
				}

				// force queue to be emptied
				serviceDeploymentQueue()

				depID, err := getCurrentDeploymentID()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(depID).Should(Equal(deploymentID))

				dep, err := getDeployment(depID)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(dep.System.URI).To(Equal(dep.System.URI))
				Expect(len(dep.Bundles)).To(Equal(len(dep.Bundles)))
				Expect(dep.Bundles[0].URI).To(Equal(getBundleFilePath(deploymentID, bundleUri)))

				close(done)
			},
		}

		apid.Events().Listen(APIGEE_SYNC_EVENT, h)
		apid.Events().Emit(APIGEE_SYNC_EVENT, &event)              // for standard listener
		apid.Events().Emit(APIGEE_SYNC_EVENT, &common.Snapshot{}) // for test listener
 	})

	It("should process ApigeeSync change event", func(done Done) {

		deploymentID := "listener_test_2"

		uri, err := url.Parse(testServer.URL)
		Expect(err).ShouldNot(HaveOccurred())
		uri.Path = "/bundle"
		bundleUri := uri.String()

		man := bundleManifest{
			SysBun: systemBundle{
				URI: bundleUri,
			},
			DepBun: []dependantBundle{
				{
					URI: bundleUri,
					Scope: "some-scope",
				},
			},
		}
		manBytes, err := json.Marshal(man)
		Expect(err).ShouldNot(HaveOccurred())
		manifest := string(manBytes)

		row := common.Row{}
		row["id"] = &common.ColumnVal{Value: deploymentID}
		row["body"] = &common.ColumnVal{Value: manifest}

		var event = common.ChangeList{}
		event.Changes = []common.Change{
			{
				Operation: common.Insert,
				Table: MANIFEST_TABLE,
				NewRow: row,
			},
		}

		h := &test_handler{
			"checkDatabase",
			func(e apid.Event) {
				defer GinkgoRecover()

				// ignore the first event, let standard listener process it
				changeSet := e.(*common.ChangeList)
				if len(changeSet.Changes) > 0 {
					return
				}

				// force queue to be emptied
				serviceDeploymentQueue()

				depID, err := getCurrentDeploymentID()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(depID).Should(Equal(deploymentID))

				dep, err := getDeployment(depID)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(dep.System.URI).To(Equal(man.SysBun.URI))
				Expect(len(dep.Bundles)).To(Equal(len(man.DepBun)))
				Expect(dep.Bundles[0].URI).To(Equal(getBundleFilePath(deploymentID, bundleUri)))

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
