package apiGatewayDeploy

import (
	"encoding/json"
	"github.com/30x/apid"
	. "github.com/30x/apidApigeeSync" // for direct access to Payload types
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/url"
	"time"
)

var _ = Describe("listener", func() {

	It("should store data from ApigeeSync in the database", func(done Done) {

		deploymentID := "listener_test_1"

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
				},
			},
		}
		manBytes, err := json.Marshal(man)
		Expect(err).ShouldNot(HaveOccurred())
		manifest := string(manBytes)

		now := time.Now().Unix()
		var event = ChangeSet{}
		event.Changes = []ChangePayload{
			{
				Data: DataPayload{
					EntityType:       "deployment",
					Operation:        "create",
					EntityIdentifier: deploymentID,
					PldCont: Payload{
						CreatedAt: now,
						Manifest:  manifest,
					},
				},
			},
		}

		h := &test_handler{
			"checkDatabase",
			func(e apid.Event) {

				// ignore the first event, let standard listener process it
				changeSet := e.(*ChangeSet)
				if len(changeSet.Changes) > 0 {
					return
				}

				depID, err := getCurrentDeploymentID()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(depID).Should(Equal(deploymentID))

				dep, err := getDeployment(depID)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(dep.System.URI).To(Equal(man.SysBun.URI))
				Expect(len(dep.Bundles)).To(Equal(len(man.DepBun)))
				Expect(dep.Bundles[0].URI).To(Equal(getBundleFilePath(deploymentID, bundleUri)))

				// todo: should do a lot more checking here... maybe call another api instead?
				//var selectedManifest string
				//var createdAt int64
				//err = db.QueryRow("SELECT manifest, created_at from bundle_deployment where id = ?", deploymentID).
				//	Scan(&selectedManifest, &createdAt)
				//Expect(err).ShouldNot(HaveOccurred())
				//
				//Expect(manifest).Should(Equal(selectedManifest))
				//Expect(createdAt).Should(Equal(now))

				// clean up
				//_, err = db.Exec("DELETE from bundle_deployment where id = ?", deploymentID)
				//Expect(err).ShouldNot(HaveOccurred())

				close(done)
			},
		}

		apid.Events().Listen(ApigeeSyncEventSelector, h)
		apid.Events().Emit(ApigeeSyncEventSelector, &event)       // for standard listener
		apid.Events().Emit(ApigeeSyncEventSelector, &ChangeSet{}) // for test listener
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
