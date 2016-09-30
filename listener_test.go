package apiGatewayDeploy

import (
	"github.com/30x/apid"
	. "github.com/30x/apidApigeeSync" // for direct access to Payload types
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("listener", func() {

	It("should store data from ApigeeSync in the database", func(done Done) {

		now := time.Now().Unix()
		var event = ChangeSet{}
		event.Changes = []ChangePayload{
			{
				Data: DataPayload{
					EntityType: "deployment",
					Operation: "create",
					EntityIdentifier: "entityID",
					PldCont: Payload{
						CreatedAt: now,
						Manifest: "manifest",
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

				db, err := data.DB()
				Expect(err).NotTo(HaveOccurred())

				// todo: should do a lot more checking here... maybe call another api instead?
				var manifest string
				var createdAt int64
				err = db.QueryRow("SELECT manifest, created_at from bundle_deployment where id = ?", "entityID").
					Scan(&manifest, &createdAt)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(manifest).Should(Equal("manifest"))
				Expect(createdAt).Should(Equal(now))

				close(done)
			},
		}

		apid.Events().Listen(ApigeeSyncEventSelector, h)
		apid.Events().Emit(ApigeeSyncEventSelector, &event) // for standard listener
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
