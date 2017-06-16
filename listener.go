package apiGatewayDeploy

import (
	"os"
	"time"

	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
	"database/sql"
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
		log.Debugf("Received invalid event. Ignoring. %v", e)
	}
}

func processSnapshot(snapshot *common.Snapshot) {

	log.Debugf("Snapshot received. Switching to DB version: %s", snapshot.SnapshotInfo)

	db, err := data.DBVersion(snapshot.SnapshotInfo)
	if err != nil {
		log.Panicf("Unable to access database: %v", err)
	}

	// Update the DB pointer
	dbMux.Lock()
	SetDB(db)
	dbMux.Unlock()

	InitDB(db)
	startupOnExistingDatabase()
	log.Debug("Snapshot processed")
}

func startupOnExistingDatabase() {
	// start bundle downloads that didn't finish
	go func() {
		deployments, err := getUnreadyDeployments()

		if err != nil && err != sql.ErrNoRows {
			log.Panicf("unable to query database for unready deployments: %v", err)
		}
		log.Debugf("Queuing %d deployments for bundle download", len(deployments))
		for _, dep := range deployments {
			queueDownloadRequest(dep)
		}
	}()
}

func processChangeList(changes *common.ChangeList) {

	// changes have been applied to DB
	var insertedDeployments, deletedDeployments []DataDeployment
	for _, change := range changes.Changes {
		switch change.Table {
		case DEPLOYMENT_TABLE:
			switch change.Operation {
			case common.Insert:
				dep, err := dataDeploymentFromRow(change.NewRow)
				if err == nil {
					insertedDeployments = append(insertedDeployments, dep)
				}
			case common.Delete:
				var id, dataScopeID string
				change.OldRow.Get("id", &id)
				change.OldRow.Get("data_scope_id", &dataScopeID)
				// only need these two fields to delete and determine bundle file
				dep := DataDeployment{
					ID:          id,
					DataScopeID: dataScopeID,
				}
				deletedDeployments = append(deletedDeployments, dep)
			default:
				log.Errorf("unexpected operation: %s", change.Operation)
			}
		}
	}

	for _, d := range deletedDeployments {
		deploymentsChanged <- d.ID
	}

	log.Debug("ChangeList processed")

	for _, dep := range insertedDeployments {
		queueDownloadRequest(dep)
	}

	// clean up old bundles
	if len(deletedDeployments) > 0 {
		log.Debugf("will delete %d old bundles", len(deletedDeployments))
		go func() {
			// give clients a minute to avoid conflicts
			time.Sleep(bundleCleanupDelay)
			for _, dep := range deletedDeployments {
				bundleFile := getBundleFile(dep)
				log.Debugf("removing old bundle: %v", bundleFile)
				safeDelete(bundleFile)
			}
		}()
	}
}

func dataDeploymentFromRow(row common.Row) (d DataDeployment, err error) {

	row.Get("id", &d.ID)
	row.Get("org_id", &d.OrgID)
	row.Get("env_id", &d.EnvID)
	row.Get("data_scope_id", &d.DataScopeID)
	row.Get("bundle_config_json", &d.Type)
	row.Get("config_json", &d.Name)
	row.Get("created", &d.Revision)
	row.Get("created_by", &d.BlobID)
	row.Get("created_by", &d.BlobResourceID)
	row.Get("created_by", &d.Updated)
	row.Get("created_by", &d.UpdatedBy)
	row.Get("created_by", &d.Created)
	row.Get("updated", &d.CreatedBy)

	return
}

func safeDelete(file string) {
	if e := os.Remove(file); e != nil && !os.IsNotExist(e) {
		log.Warnf("unable to delete file %s: %v", file, e)
	}
}
