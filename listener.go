package apiGatewayDeploy

import (
	"encoding/json"
	"os"
	"time"

	"fmt"

	"github.com/30x/apid-core"
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
		log.Debugf("Received invalid event. Ignoring. %v", e)
	}
}

func processSnapshot(snapshot *common.Snapshot) {

	log.Debugf("Snapshot received. Switching to DB version: %s", snapshot.SnapshotInfo)

	db, err := data.DBVersion(snapshot.SnapshotInfo)
	if err != nil {
		log.Panicf("Unable to access database: %v", err)
	}

	// ensure that no new database updates are made on old database
	dbMux.Lock()
	SetDB(db)
	dbMux.Unlock()
	startupOnExistingDatabase()
	log.Debug("Snapshot processed")
}

func startupOnExistingDatabase() {
	// ensure all deployment statuses have been sent to tracker
	go func() {
		deployments, err := getDeployments("WHERE deploy_status != $1", "")
		if err != nil {
			log.Panicf("unable to query database for ready deployments: %v", err)
		}
		log.Debugf("Queuing %d deployments for bundle download", len(deployments))

		var results apiDeploymentResults
		for _, dep := range deployments {
			result := apiDeploymentResult{
				ID:        dep.ID,
				Status:    dep.DeployStatus,
				ErrorCode: dep.DeployErrorCode,
				Message:   dep.DeployErrorMessage,
			}
			results = append(results, result)
		}
		if len(results) > 0 {
			transmitDeploymentResultsToServer(results)
		}
	}()

	// start bundle downloads that didn't finish
	go func() {
		deployments, err := getUnreadyDeployments()
		if err != nil {
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
	var errResults apiDeploymentResults
	for _, change := range changes.Changes {
		switch change.Table {
		case DEPLOYMENT_TABLE:
			switch change.Operation {
			case common.Insert:
				dep, err := dataDeploymentFromRow(change.NewRow)
				if err == nil {
					insertedDeployments = append(insertedDeployments, dep)
				} else {
					result := apiDeploymentResult{
						ID:        dep.ID,
						Status:    RESPONSE_STATUS_FAIL,
						ErrorCode: TRACKER_ERR_DEPLOYMENT_BAD_JSON,
						Message:   fmt.Sprintf("unable to parse deployment: %v", err),
					}
					errResults = append(errResults, result)
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

	// transmit parsing errors back immediately
	if len(errResults) > 0 {
		go transmitDeploymentResultsToServer(errResults)
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

func safeDelete(file string) {
	if e := os.Remove(file); e != nil && !os.IsNotExist(e) {
		log.Warnf("unable to delete file %s: %v", file, e)
	}
}
