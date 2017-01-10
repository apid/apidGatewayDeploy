package apiGatewayDeploy

import (
	"database/sql"
	"encoding/json"
	"github.com/30x/apid"
	"github.com/apigee-labs/transicator/common"
)

const (
	APIGEE_SYNC_EVENT = "ApigeeSync"
	DEPLOYMENT_TABLE  = "edgex.deployment"
)

func initListener(services apid.Services) {
	services.Events().Listen(APIGEE_SYNC_EVENT, &apigeeSyncHandler{})
}

type bundleConfigJson struct {
	Name         string `json:"name"`
	URI          string `json:"uri"`
	ChecksumType string `json:"checksumType"`
	Checksum     string `json:"checksum"`
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

	tx, err := db.Begin()
	defer tx.Rollback()
	for _, table := range snapshot.Tables {
		var err error
		switch table.Name {
		case DEPLOYMENT_TABLE:
			log.Debugf("Snapshot of %s with %d rows", table.Name, len(table.Rows))
			if len(table.Rows) == 0 {
				return
			}
			for _, row := range table.Rows {
				addDeployment(tx, row)
			}
		}
		if err != nil {
			log.Panicf("Error processing Snapshot: %v", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Panicf("Error committing Snapshot change: %v", err)
	}

	setDB(db)
	log.Debug("Snapshot processed")
}

func processChangeList(changes *common.ChangeList) {

	tx, err := getDB().Begin()
	if err != nil {
		log.Panicf("Error processing ChangeList: %v", err)
	}
	defer tx.Rollback()
	for _, change := range changes.Changes {
		var err error
		switch change.Table {
		case DEPLOYMENT_TABLE:
			switch change.Operation {
			case common.Insert:
				err = addDeployment(tx, change.NewRow)
			case common.Delete:
				var id string
				err = change.OldRow.Get("id", &id)
				if err == nil {
					err = deleteDeployment(tx, id)
					// todo: delete downloaded bundle file
				}
			default:
				log.Errorf("unexpected operation: %s", change.Operation)
			}
		}
		if err != nil {
			log.Panicf("Error processing ChangeList: %v", err)
		}
	}
	err = tx.Commit()
	if err != nil {
		log.Panicf("Error processing ChangeList: %v", err)
	}
}

func addDeployment(tx *sql.Tx, row common.Row) (err error) {

	d := dataDeployment{}
	err = row.Get("id", &d.ID)
	if err != nil {
		return
	}
	err = row.Get("bundle_config_id", &d.BundleConfigID)
	if err != nil {
		return
	}
	err = row.Get("apid_cluster_id", &d.ApidClusterID)
	if err != nil {
		return
	}
	err = row.Get("data_scope_id", &d.DataScopeID)
	if err != nil {
		return
	}
	err = row.Get("bundle_config_json", &d.BundleConfigJSON)
	if err != nil {
		return
	}
	err = row.Get("config_json", &d.ConfigJSON)
	if err != nil {
		return
	}
	err = row.Get("status", &d.Status)
	if err != nil {
		return
	}
	err = row.Get("created", &d.Created)
	if err != nil {
		return
	}
	err = row.Get("created_by", &d.CreatedBy)
	if err != nil {
		return
	}
	err = row.Get("updated", &d.Updated)
	if err != nil {
		return
	}
	err = row.Get("updated_by", &d.UpdatedBy)
	if err != nil {
		return
	}

	var bc bundleConfigJson
	err = json.Unmarshal([]byte(d.BundleConfigJSON), &bc)
	if err != nil {
		log.Errorf("JSON decoding Manifest failed: %v", err)
		return
	}

	d.BundleName = bc.Name
	d.BundleURI = bc.URI
	d.BundleChecksumType = bc.ChecksumType
	d.BundleChecksum = bc.Checksum

	err = insertDeployment(tx, d)
	if err != nil {
		return
	}

	// todo: limit # concurrent downloads?
	go downloadBundle(d)
	return
}
