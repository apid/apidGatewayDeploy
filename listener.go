package apiGatewayDeploy

import (
	"database/sql"
	"encoding/json"
	"github.com/30x/apid"
	"github.com/apigee-labs/transicator/common"
	"os"
	"time"
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

	err = InitDB(db)
	if err != nil {
		log.Panicf("Unable to initialize database: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		log.Panicf("Error starting transaction: %v", err)
	}

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

	SetDB(db)

	// if no tables, this a startup event for an existing DB, start bundle downloads that didn't finish
	if len(snapshot.Tables) == 0 {
		deployments, err := getUnreadyDeployments()
		if err != nil {
			log.Panicf("unable to query database for unready deployments: %v", err)
		}
		for _, dep := range deployments {
			go downloadBundle(dep)
		}
	}

	log.Debug("Snapshot processed")
}

func processChangeList(changes *common.ChangeList) {

	tx, err := getDB().Begin()
	if err != nil {
		log.Panicf("Error processing ChangeList: %v", err)
	}
	defer tx.Rollback()
	var bundlesToDelete []string
	for _, change := range changes.Changes {
		var err error
		switch change.Table {
		case DEPLOYMENT_TABLE:
			switch change.Operation {
			case common.Insert:
				err = addDeployment(tx, change.NewRow)
			case common.Delete:
				var id string
				change.OldRow.Get("id", &id)
				localBundleUri, err := getLocalBundleURI(tx, id)
				if err == nil {
					bundlesToDelete = append(bundlesToDelete, localBundleUri)
					err = deleteDeployment(tx, id)
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

	// clean up old bundles
	if len(bundlesToDelete) > 0 {
		log.Debugf("will delete %d old bundles", len(bundlesToDelete))
		go func() {
			// give clients a minute to avoid conflicts
			time.Sleep(bundleCleanupDelay)
			for _, b := range bundlesToDelete {
				log.Debugf("removing old bundle: %v", b)
				safeDelete(b)
			}
		}()
	}
}

func dataDeploymentFromRow(row common.Row) (d DataDeployment, err error) {

	row.Get("id", &d.ID)
	row.Get("bundle_config_id", &d.BundleConfigID)
	row.Get("apid_cluster_id", &d.ApidClusterID)
	row.Get("data_scope_id", &d.DataScopeID)
	row.Get("bundle_config_json", &d.BundleConfigJSON)
	row.Get("config_json", &d.ConfigJSON)
	row.Get("created", &d.Created)
	row.Get("created_by", &d.CreatedBy)
	row.Get("updated", &d.Updated)
	row.Get("updated_by", &d.UpdatedBy)

	var bc bundleConfigJson
	json.Unmarshal([]byte(d.BundleConfigJSON), &bc)
	if err != nil {
		log.Errorf("JSON decoding Manifest failed: %v", err)
		return
	}

	d.BundleName = bc.Name
	d.BundleURI = bc.URI
	d.BundleChecksumType = bc.ChecksumType
	d.BundleChecksum = bc.Checksum

	return
}

func addDeployment(tx *sql.Tx, row common.Row) (err error) {

	var d DataDeployment
	d, err = dataDeploymentFromRow(row)
	if err != nil {
		return
	}

	err = InsertDeployment(tx, d)
	if err != nil {
		return
	}

	// todo: limit # concurrent downloads?
	go downloadBundle(d)
	return
}

func safeDelete(file string) {
	if e := os.Remove(file); e != nil && !os.IsNotExist(e) {
		log.Warnf("unable to delete file %s: %v", file, e)
	}
}
