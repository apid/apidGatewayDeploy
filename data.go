package apiGatewayDeploy

import (
	"database/sql"
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

type SQLExec interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// todo: should we have some kind of deployment log instead of just a status?

func initDB() {

	var count int
	row := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='gateway_deploy_deployment';")
	if err := row.Scan(&count); err != nil {
		log.Panicf("Unable to check for tables: %v", err)
	}
	if count > 0 {
		return
	}

	log.Debug("Creating database tables...")

	tx, err := db.Begin()
	if err != nil {
		log.Panicf("Unable to start transaction: %v", err)
	}
	defer tx.Rollback()

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
	} else {
		log.Debug("Database tables created.")
	}
}

func dbTimeNow() int64 {
	return int64(time.Now().UnixNano())
}

func insertDeployment(depID string, dep deployment) error {

	log.Debugf("insertDeployment: %s", depID)

	tx, err := db.Begin()
	defer tx.Rollback()
	if err != nil {
		log.Errorf("insertDeployment begin transaction failed: %v", depID, err)
		return err
	}

	timeNow := dbTimeNow()

	_, err = tx.Exec("INSERT INTO gateway_deploy_deployment " +
		"(id, status, created_at) VALUES(?,?,?);",
		depID, DEPLOYMENT_STATE_READY, timeNow)
	if err != nil {
		log.Errorf("INSERT gateway_deploy_deployment %s failed: %v", depID, err)
		return err
	}

	// system bundle
	// todo: extra data?
	_, err = tx.Exec("INSERT INTO gateway_deploy_bundle " +
		"(id, deployment_id, scope, type, uri, status, created_at) " +
		"VALUES(?,?,?,?,?,?,?);",
		dep.System.BundleID, depID, dep.System.Scope, BUNDLE_TYPE_SYS, dep.System.URI, DEPLOYMENT_STATE_READY, timeNow)
	if err != nil {
		log.Errorf("INSERT gateway_deploy_bundle %s:%s failed: %v", depID, dep.System.BundleID, err)
		return err
	}

	// todo: extra data?
	for _, bun := range dep.Bundles {
		_, err = tx.Exec("INSERT INTO gateway_deploy_bundle " +
			"(id, deployment_id, scope, type, uri, status, created_at) " +
			"VALUES(?,?,?,?,?,?,?);",
			bun.BundleID, depID, bun.Scope, BUNDLE_TYPE_DEP, bun.URI, DEPLOYMENT_STATE_INPROG, timeNow)
		if err != nil {
			log.Errorf("INSERT gateway_deploy_bundle %s:%s failed: %v", depID, bun.BundleID, err)
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Errorf("commit insert to gateway_deploy_bundle %s failed: %v", depID, err)
	}

	log.Debugf("INSERT gateway_deploy_deployment %s succeeded", depID)

	incoming <- depID
	return err
}

func updateDeploymentAndBundles(depID string, rsp deploymentResponse) error {

	log.Debugf("updateDeploymentAndBundles: %s", depID)

	/*
	 * If the state of deployment was success, update state of bundles and
	 * its deployments as success as well
	 */
	log.Print("begin 3")
	txn, err := db.Begin()
	if err != nil {
		log.Errorf("Unable to begin transaction: %s", err)
		return err
	}
	log.Print("began 3")
	defer txn.Rollback()

	if rsp.Status == RESPONSE_STATUS_SUCCESS {
		err := updateDeploymentStatus(txn, depID, DEPLOYMENT_STATE_SUCCESS, 0)
		if err != nil {
			return err
		}
		err = updateAllBundleStatus(txn, depID, DEPLOYMENT_STATE_SUCCESS)
		if err != nil {
			return err
		}
	} else {
		err := updateDeploymentStatus(txn, depID, DEPLOYMENT_STATE_ERR_GWY, rsp.Error.ErrorCode)
		if err != nil {
			return err
		}

		// Iterate over Bundles, and update the errors
		for _, a := range rsp.Error.ErrorDetails {
			updateBundleStatus(txn, depID, a.BundleID, DEPLOYMENT_STATE_ERR_GWY, a.ErrorCode, a.Reason)
			if err != nil {
				return err
			}
		}
	}
	if err != nil {
		return err
	}

	log.Print("commit 3")
	err = txn.Commit()
	if err != nil {
		log.Errorf("Unable to commit updateDeploymentStatus transaction: %s", err)
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
		log.Errorf("UPDATE gateway_deploy_bundle %s:%s failed: %v", depID, bundleID, err)
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

	rows, err := db.Query("SELECT id, type, uri, COALESCE(scope, '') as scope " +
		"FROM gateway_deploy_bundle WHERE deployment_id=?;", depID)
	if err != nil {
		log.Errorf("Unable to query gateway_deploy_bundle. Err: %s", err)
		return nil, err
	}
	defer rows.Close()

	depRes := deployment{
		Bundles:      []bundle{},
		DeploymentID: depID,
	}

	for rows.Next() {
		var bundleType int
		var bundleID, uri, scope string
		err = rows.Scan(&bundleID, &bundleType, &uri, &scope)
		if err != nil {
			log.Errorf("gateway_deploy_bundle fetch failed. Err: %s", err)
			return nil, err
		}
		if bundleType == BUNDLE_TYPE_SYS {
			depRes.System = bundle{
				BundleID: bundleID,
				URI:      uri,
			}
		} else {
			fileUrl := getBundleFilePath(depID, uri)
			bd := bundle{
				BundleID: bundleID,
				URI:      fileUrl,
				Scope:    scope,
			}
			depRes.Bundles = append(depRes.Bundles, bd)
		}
	}
	return &depRes, nil
}
