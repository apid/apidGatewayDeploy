package apiGatewayDeploy

import (
	"database/sql"
	"github.com/30x/apidApigeeSync"
	"time"
)

const (
	DEPLOYMENT_STATE_INPROG = 1
	DEPLOYMENT_STATE_ERR_APID = 2
	DEPLOYMENT_STATE_ERR_GWY = 3
	DEPLOYMENT_STATE_READY = 4
	DEPLOYMENT_STATE_SUCCESS = 5

	BUNDLE_TYPE_SYS = 1
	BUNDLE_TYPE_DEP = 2
)

/*
Startup flow:
	Check deployment queue
	If anything in queue, initiate deployment retrieval
Listener flow:
	Receive deployment event
	Store deployment event in deployment queue
	Initiate deployment retrieval
Deployment Retrieval:
	Load deployment from deployment queue
	Retrieve and store each bundle
	Mark deployment as ready to deploy
	Trigger deployment
Deployment:

Tables:
	gateway_deploy_queue
		Deployment(s) received and not yet processed (potentially a Queue - one for now)
	gateway_deploy_deployment
	gateway_deploy_bundle
 */


type SQLExec interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// todo: should we have some kind of deployment log instead of just a status?

func initDB() {

	var count int
	row := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='gateway_deploy_queue';")
	if err := row.Scan(&count); err != nil {
		log.Panicf("Unable to check for tables: %v", err)
	}
	if count > 0 {
		return
	}

	tx, err := db.Begin()
	if err != nil {
		log.Panicf("Unable to start transaction: %v", err)
	}
	defer tx.Rollback()
	_, err = tx.Exec("CREATE TABLE gateway_deploy_queue (" +
		"id varchar(255), manifest text, created_at integer, " +
		"PRIMARY KEY (id));")
	if err != nil {
		log.Panicf("Unable to initialize gateway_deploy_queue: %v", err)
	}

	_, err = tx.Exec("CREATE TABLE gateway_deploy_deployment (" +
		"id varchar(255), status integer, created_at integer, " +
		"modified_at integer, error_code varchar(255), " +
		"PRIMARY KEY (id));")
	if err != nil {
		log.Panicf("Unable to initialize gateway_deploy_deployment: %v", err)
	}

	_, err = tx.Exec("CREATE TABLE gateway_deploy_bundle (" +
		"deployment_id varchar(255), id varchar(255), scope varchar(255), uri varchar(255), type integer, " +
		"created_at integer, modified_at integer, status integer, error_code integer, error_reason text, " +
		"PRIMARY KEY (deployment_id, id), " +
		"FOREIGN KEY (deployment_id) references gateway_deploy_deployment(id) ON DELETE CASCADE);")
	if err != nil {
		log.Panicf("Unable to initialize gateway_deploy_bundle: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		log.Panicf("Unable to commit transaction: %v", err)
	}
}

// currently only maintains 1 in the queue
func queueDeployment(payload apidApigeeSync.DataPayload) error {

	// todo: validate payload manifest

	// maintains queue at 1
	tx, err := db.Begin()
	if err != nil {
		log.Debugf("INSERT gateway_deploy_queue failed: (%s)", payload.EntityIdentifier)
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec("DELETE FROM gateway_deploy_queue");
	if err != nil {
		log.Errorf("DELETE FROM gateway_deploy_queue failed: %v", err)
		return err
	}

	_, err = tx.Exec("INSERT INTO gateway_deploy_queue (id, manifest, created_at) VALUES (?,?,?);",
		payload.EntityIdentifier,
		payload.PldCont.Manifest,
		payload.PldCont.CreatedAt,
	)
	if err != nil {
		log.Errorf("INSERT gateway_deploy_queue %s failed: %v", payload.EntityIdentifier, err)
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Errorf("INSERT gateway_deploy_queue %s failed: %v", payload.EntityIdentifier, err)
		return err
	}

	log.Debugf("INSERT gateway_deploy_queue success: (%s)", payload.EntityIdentifier)

	return nil
}

func getQueuedDeployment() (depID, manifestString string) {
	err := db.QueryRow("SELECT id, manifest FROM gateway_deploy_queue ORDER BY created_at ASC LIMIT 1;").
		Scan(&depID, &manifestString)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Info("No Deployments available to be processed")
		} else {
			// todo: panic?
			log.Errorf("SELECT on BUNDLE_DEPLOYMENT failed with Err: %s", err)
		}
	}
	return
}

func dequeueDeployment(depID string) error {
	_, err := db.Exec("DELETE FROM gateway_deploy_queue WHERE id=?;", depID)
	return err
}

func dbTimeNow() int64 {
	return int64(time.Now().UnixNano())
}

func insertDeployment(depID string, manifest bundleManifest) error {

	tx, err := db.Begin()
	if err != nil {
		log.Errorf("INSERT gateway_deploy_deployment %s failed: %v", depID, err)
		return err
	}
	defer tx.Rollback()

	timeNow := dbTimeNow()

	_, err = tx.Exec("INSERT INTO gateway_deploy_deployment " +
		"(id, status, created_at) VALUES(?,?,?);",
		depID, DEPLOYMENT_STATE_INPROG, timeNow)
	if err != nil {
		log.Errorf("INSERT gateway_deploy_deployment %s failed: %v", depID, err)
		return err
	}

	// system bundle
	// todo: extra data?
	_, err = tx.Exec("INSERT INTO gateway_deploy_bundle " +
		"(id, deployment_id, type, uri, status, created_at) " +
		"VALUES(?,?,?,?,?,?);",
		"sys", depID, BUNDLE_TYPE_SYS, manifest.SysBun.URI, DEPLOYMENT_STATE_INPROG, timeNow)
	if err != nil {
		log.Errorf("INSERT gateway_deploy_bundle %s:%s failed: %v", depID, "sys", err)
		return err
	}

	// todo: extra data?
	for i, bun := range manifest.DepBun {
		id := string(i)
		_, err = tx.Exec("INSERT INTO gateway_deploy_bundle " +
			"(id, deployment_id, scope, type, uri, status, created_at) " +
			"VALUES(?,?,?,?,?,?,?);",
			id, depID, bun.Scope, BUNDLE_TYPE_DEP, bun.URI, DEPLOYMENT_STATE_INPROG, timeNow)
		if err != nil {
			log.Errorf("INSERT gateway_deploy_bundle %s:%s failed: %v", depID, id, err)
			return err
		}
	}

	log.Debugf("INSERT gateway_deploy_deployment %s succeeded", depID)

	err = tx.Commit()
	if err != nil {
		log.Errorf("INSERT gateway_deploy_bundle %s failed: %v", depID)
	}

	return err
}

func updateDeploymentStatus(txn SQLExec, depID string, status int, errCode int) error {
	var nRows int64
	res, err := txn.Exec("UPDATE gateway_deploy_deployment " +
		"SET status=?, modified_at=?, error_code = ? WHERE id=?;", status, dbTimeNow(), errCode, depID)
	if err == nil {
		nRows, err = res.RowsAffected()
		if nRows == 0 {
			err = sql.ErrNoRows
		}
	}
	if err != nil {
		log.Errorf("UPDATE gateway_deploy_deployment %s failed: %v", depID, err)
		return err
	}

	log.Debugf("UPDATE gateway_deploy_deployment %s succeeded", depID)
	return nil
}

func updateAllBundleStatus(txn SQLExec, depID string, status int) error {
	var nRows int64
	res, err := txn.Exec("UPDATE gateway_deploy_bundle SET status = ? WHERE deployment_id = ?;", status, depID)
	if err == nil {
		nRows, err = res.RowsAffected()
		if nRows == 0 {
			err = sql.ErrNoRows
		}
	}
	if err != nil {
		log.Errorf("UPDATE all gateway_deploy_bundle %s failed: %v", depID, err)
		return err
	}

	return nil
}

func updateBundleStatus(txn SQLExec, depID string, bundleID string, status int, errCode int, errReason string) error {
	var nRows int64
	res, err := txn.Exec("UPDATE gateway_deploy_bundle " +
		"SET status=?, error_code=?, error_reason=?, modified_at=? WHERE id=?;",
		status, errCode, errReason, dbTimeNow(), depID)
	if err == nil {
		nRows, err = res.RowsAffected()
		if nRows == 0 {
			err = sql.ErrNoRows
		}
	}
	if err != nil {
		log.Error("UPDATE gateway_deploy_bundle %s:%s failed: %v", depID, bundleID, err)
		return err
	}

	log.Debugf("UPDATE gateway_deploy_bundle success: %s:%s", depID, bundleID)
	return nil
}

// getCurrentDeploymentID returns the ID of what should be the "current" deployment
func getCurrentDeploymentID() (string, error) {
	var depID string
	err := db.QueryRow("SELECT id FROM gateway_deploy_deployment " +
		"WHERE status >= ? ORDER BY created_at DESC LIMIT 1;", DEPLOYMENT_STATE_READY).Scan(&depID)
	log.Debugf("current deployment id: %s", depID)
	return depID, err
}

// getDeployment returns a fully populated deploymentResponse
func getDeployment(depID string) (*deployment, error) {

	rows, err := db.Query("SELECT id, type, uri FROM gateway_deploy_bundle WHERE deployment_id=?;", depID)
	if err != nil {
		log.Errorf("Unable to query gateway_deploy_bundle. Err: %s", err)
		return nil, err
	}

	depRes := deployment{
		Bundles:      []bundle{},
		DeploymentId: depID,
	}

	for rows.Next() {
		var bundleType int
		var bundleID, uri string
		err = rows.Scan(&bundleID, &bundleType, &uri)
		if err != nil {
			log.Errorf("gateway_deploy_bundle fetch failed. Err: %s", err)
			return nil, err
		}
		if bundleType == BUNDLE_TYPE_SYS {
			depRes.System = bundle{
				BundleId: bundleID,
				URI:      uri,
			}
		} else {
			fileUrl := getBundleFilePath(depID, uri)
			bd := bundle{
				AuthCode: bundleID, // todo: authCode?
				BundleId: bundleID,
				URI:      fileUrl,
			}
			depRes.Bundles = append(depRes.Bundles, bd)
		}
	}
	return &depRes, nil
}
