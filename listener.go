package apiGatewayDeploy

import (
	"github.com/30x/apidApigeeSync"
	"github.com/30x/apid"
)

type apigeeSyncHandler struct {
}

func (h *apigeeSyncHandler) String() string {
	return "gatewayDeploy"
}

func (h *apigeeSyncHandler) Handle(e apid.Event) {
	changeSet, ok := e.(*apidApigeeSync.ChangeSet)
	if !ok {
		log.Errorf("Received non-ChangeSet event. This shouldn't happen!")
		return
	}

	log.Debugf("apigeeSyncEvent: %d changes", len(changeSet.Changes))

	for _, payload := range changeSet.Changes {

		if payload.Data.EntityType != "deployment" {
			continue
		}

		switch payload.Data.Operation {
			case "create":
				insertDeployment(payload.Data)
		}

	}
}

func insertDeployment(payload apidApigeeSync.DataPayload) {

	db, err := data.DB()
	if err != nil {
		panic("help me!") // todo: handle
	}

	_, err = db.Exec("INSERT INTO BUNDLE_DEPLOYMENT (id, manifest, created_at, deploy_status) VALUES (?,?,?,?);",
		payload.EntityIdentifier,
		payload.PldCont.Manifest,
		payload.PldCont.CreatedAt,
		DEPLOYMENT_STATE_UNUSED,
	)

	if err != nil {
		log.Errorf("INSERT BUNDLE_DEPLOYMENT Failed: (%s, %v)", payload.EntityIdentifier, err)
		return
	}

	log.Infof("INSERT BUNDLE_DEPLOYMENT Success: (%s)", payload.EntityIdentifier)
	orchestrateDeploymentAndTrigger()
}
