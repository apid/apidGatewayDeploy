package apiGatewayDeploy

import (
	"github.com/30x/apid"
	"github.com/30x/apidApigeeSync"
)

func initListener(services apid.Services) {
	services.Events().Listen(apidApigeeSync.ApigeeSyncEventSelector, &apigeeSyncHandler{})
}

type apigeeSyncHandler struct {
}

func (h *apigeeSyncHandler) String() string {
	return "gatewayDeploy"
}

func (h *apigeeSyncHandler) Handle(e apid.Event) {
	changeSet, ok := e.(*apidApigeeSync.ChangeSet)
	if !ok {
		log.Errorf("Received non-ChangeSet event.")
		return
	}

	log.Debugf("apigeeSyncEvent: %d changes", len(changeSet.Changes))

	for _, payload := range changeSet.Changes {

		if payload.Data.EntityType != "deployment" {
			continue
		}

		switch payload.Data.Operation {
		case "create":
			err := queueDeployment(payload.Data)
			if err == nil {
				serviceDeploymentQueue()
			} else {
				log.Errorf("unable to queue deployment")
			}
		}

	}
}
