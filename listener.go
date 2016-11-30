package apiGatewayDeploy

import (
	"github.com/30x/apid"
	"github.com/apigee-labs/transicator/common"
)

const (
	APIGEE_SYNC_EVENT = "ApigeeSync"
	MANIFEST_TABLE = "edgex.apid_config_manifest_deployment"
)

func initListener(services apid.Services) {
	services.Events().Listen(APIGEE_SYNC_EVENT, &apigeeSyncHandler{})
}

type apigeeSyncHandler struct {
}

func (h *apigeeSyncHandler) String() string {
	return "gatewayDeploy"
}

func (h *apigeeSyncHandler) Handle(e apid.Event) {

	if changeSet, ok := e.(*common.ChangeList); ok {
		processChangeList(changeSet)
	} else if snapData, ok := e.(*common.Snapshot); ok {
		processSnapshot(snapData)
	} else {
		log.Errorf("Received invalid event. Ignoring. %v", e)
	}
}

func processSnapshot(snapshot *common.Snapshot) {

	log.Debugf("Snapshot received. Switching to DB version: %s", snapshot.SnapshotInfo)

	db, err := data.DBVersion(snapshot.SnapshotInfo)
	if err != nil {
		log.Panicf("Unable to access database: %v", err)
	}

	initDB(db)

	for _, table := range snapshot.Tables {
		var err error
		switch table.Name {
		case MANIFEST_TABLE:
			log.Debugf("Snapshot of %s with %d rows", table.Name, len(table.Rows))
			if len(table.Rows) == 0 {
				return
			}
			// todo: should be 0 or 1 *per system*!! - TBD
			row := table.Rows[len(table.Rows)-1]
			err = processNewManifest(db, row)
		}
		if err != nil {
			log.Panicf("Error processing Snapshot: %v", err)
		}
	}

	setDB(db)
	log.Debug("Snapshot processed")
}

func processChangeList(changes *common.ChangeList) {

	db := getDB()
	for _, change := range changes.Changes {
		log.Debugf("change table: %s operation: %s", change.Table, change.Operation)

		var err error
		switch change.Table {
		case MANIFEST_TABLE:
			switch change.Operation {
			case common.Insert:
				err = processNewManifest(db, change.NewRow)
			default:
				log.Error("unexpected operation: %s", change.Operation)
			}
		}
		if err != nil {
			log.Panicf("Error processing ChangeList: %v", err)
		}
	}
}

func processNewManifest(db apid.DB, row common.Row) error {

	var deploymentID, manifestString string
	err := row.Get("id", &deploymentID)
	if err != nil {
		return err
	}
	err = row.Get("manifest_body", &manifestString)
	if err != nil {
		return err
	}

	manifest, err := parseManifest(manifestString)
	if err != nil {
		log.Errorf("error parsing manifest: %v", err)
		return err
	}

	err = prepareDeployment(db, deploymentID, manifest)
	if err != nil {
		log.Errorf("serviceDeploymentQueue prepare deployment failed: %s", deploymentID)
		return err
	}

	return nil
}
