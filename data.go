// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package apiGatewayDeploy

import (
	"database/sql"
	"fmt"
	"sync"

	"encoding/json"
	"github.com/30x/apid-core"
	"strings"
)

var (
	unsafeDB apid.DB
	dbMux    sync.RWMutex
)

type DataDeployment struct {
	ID                 string
	BundleConfigID     string
	ApidClusterID      string
	DataScopeID        string
	BundleConfigJSON   string
	ConfigJSON         string
	Created            string
	CreatedBy          string
	Updated            string
	UpdatedBy          string
	BundleName         string
	BundleURI          string
	LocalBundleURI     string
	BundleChecksum     string
	BundleChecksumType string
	DeployStatus       string
	DeployErrorCode    int
	DeployErrorMessage string
}

type SQLExec interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

func InitDBFullColumns(db apid.DB) error {
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS edgex_deployment (
		id character varying(36) NOT NULL,
		bundle_config_id varchar(36) NOT NULL,
		apid_cluster_id varchar(36) NOT NULL,
		data_scope_id varchar(36) NOT NULL,
		bundle_config_json text NOT NULL,
		config_json text NOT NULL,
		created timestamp without time zone,
		created_by text,
		updated timestamp without time zone,
		updated_by text,
		bundle_config_name text,
		bundle_uri text,
		local_bundle_uri text,
		bundle_checksum text,
		bundle_checksum_type text,
		deploy_status string,
		deploy_error_code int,
		deploy_error_message text,
		PRIMARY KEY (id)
	);
	`)
	if err != nil {
		return err
	}

	log.Debug("Database tables created.")
	return nil
}

func InitDB(db apid.DB) error {
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS edgex_deployment (
		id character varying(36) NOT NULL,
		bundle_config_id varchar(36) NOT NULL,
		apid_cluster_id varchar(36) NOT NULL,
		data_scope_id varchar(36) NOT NULL,
		bundle_config_json text NOT NULL,
		config_json text NOT NULL,
		created timestamp without time zone,
		created_by text,
		updated timestamp without time zone,
		updated_by text,
		bundle_config_name text,
		PRIMARY KEY (id)
	);
	`)
	if err != nil {
		return err
	}

	log.Debug("Database tables created.")
	return nil
}

func alterTable(db apid.DB) error {
	queries := []string{
		"ALTER TABLE edgex_deployment ADD COLUMN bundle_uri text;",
		"ALTER TABLE edgex_deployment ADD COLUMN local_bundle_uri text;",
		"ALTER TABLE edgex_deployment ADD COLUMN bundle_checksum text;",
		"ALTER TABLE edgex_deployment ADD COLUMN bundle_checksum_type text;",
		"ALTER TABLE edgex_deployment ADD COLUMN deploy_status string;",
		"ALTER TABLE edgex_deployment ADD COLUMN deploy_error_code int;",
		"ALTER TABLE edgex_deployment ADD COLUMN deploy_error_message text;",
	}

	for _, query := range queries {
		_, err := db.Exec(query)
		if err != nil {
			if strings.Contains(err.Error(), "duplicate column name") {
				log.Warnf("AlterTable warning: %s", err)
			} else {
				return err
			}
		}
	}
	log.Debug("Database table altered.")
	return nil
}

func getDB() apid.DB {
	dbMux.RLock()
	db := unsafeDB
	dbMux.RUnlock()
	return db
}

// caller is responsible for calling dbMux.Lock() and dbMux.Unlock()
func SetDB(db apid.DB) {
	if unsafeDB == nil { // init API when DB is initialized
		go InitAPI()
	}
	unsafeDB = db
}

func InsertDeployment(tx *sql.Tx, dep DataDeployment) error {
	return insertDeployments(tx, []DataDeployment{dep})
}

func insertDeployments(tx *sql.Tx, deps []DataDeployment) error {

	log.Debugf("inserting %d edgex_deployment", len(deps))

	stmt, err := tx.Prepare(`
	INSERT INTO edgex_deployment
		(id, bundle_config_id, apid_cluster_id, data_scope_id,
		bundle_config_json, config_json, created, created_by,
		updated, updated_by, bundle_config_name, bundle_uri, local_bundle_uri,
		bundle_checksum, bundle_checksum_type, deploy_status,
		deploy_error_code, deploy_error_message)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18);
	`)
	if err != nil {
		log.Errorf("prepare insert into edgex_deployment failed: %v", err)
		return err
	}
	defer stmt.Close()

	for _, dep := range deps {
		log.Debugf("insertDeployment: %s", dep.ID)

		_, err = stmt.Exec(
			dep.ID, dep.BundleConfigID, dep.ApidClusterID, dep.DataScopeID,
			dep.BundleConfigJSON, dep.ConfigJSON, dep.Created, dep.CreatedBy,
			dep.Updated, dep.UpdatedBy, dep.BundleName, dep.BundleURI,
			dep.LocalBundleURI, dep.BundleChecksum, dep.BundleChecksumType, dep.DeployStatus,
			dep.DeployErrorCode, dep.DeployErrorMessage)
		if err != nil {
			log.Errorf("insert into edgex_deployment %s failed: %v", dep.ID, err)
			return err
		}
	}

	log.Debug("inserting edgex_deployment succeeded")
	return err
}

func updateDeploymentsColumns(tx *sql.Tx, deps []DataDeployment) error {

	log.Debugf("updating %d edgex_deployment", len(deps))

	stmt, err := tx.Prepare(`
	UPDATE edgex_deployment SET
		(bundle_uri, local_bundle_uri,
		bundle_checksum, bundle_checksum_type, deploy_status,
		deploy_error_code, deploy_error_message)
		= ($1,$2,$3,$4,$5,$6,$7) WHERE id = $8
	`)
	if err != nil {
		log.Errorf("prepare update edgex_deployment failed: %v", err)
		return err
	}
	defer stmt.Close()

	for _, dep := range deps {
		log.Debugf("updateDeploymentsColumns: processing deployment %s, %v", dep.ID, dep.BundleURI)

		_, err = stmt.Exec(
			dep.BundleURI,
			dep.LocalBundleURI, dep.BundleChecksum, dep.BundleChecksumType, dep.DeployStatus,
			dep.DeployErrorCode, dep.DeployErrorMessage, dep.ID)
		if err != nil {
			log.Errorf("updateDeploymentsColumns of edgex_deployment %s failed: %v", dep.ID, err)
			return err
		}
	}

	log.Debug("updateDeploymentsColumns of edgex_deployment succeeded")
	return err
}

func getDeploymentsToUpdate(db apid.DB) (deployments []DataDeployment, err error) {
	deployments, err = getDeployments("WHERE bundle_uri IS NULL AND local_bundle_uri IS NULL AND deploy_status IS NULL")
	if err != nil {
		log.Errorf("getDeployments in getDeploymentsToUpdate failed: %v", err)
		return
	}
	var bc bundleConfigJson
	for i, _ := range deployments {
		log.Debugf("getDeploymentsToUpdate: processing deployment %v, %v", deployments[i].ID, deployments[i].BundleConfigJSON)
		json.Unmarshal([]byte(deployments[i].BundleConfigJSON), &bc)
		if err != nil {
			log.Errorf("JSON decoding Manifest failed: %v", err)
			return
		}
		deployments[i].BundleName = bc.Name
		deployments[i].BundleURI = bc.URI
		deployments[i].BundleChecksumType = bc.ChecksumType
		deployments[i].BundleChecksum = bc.Checksum

		log.Debugf("Unmarshal: %v", deployments[i].BundleURI)
	}
	return
}

func deleteDeployment(tx *sql.Tx, depID string) error {

	log.Debugf("deleteDeployment: %s", depID)

	stmt, err := tx.Prepare("DELETE FROM edgex_deployment where id = $1;")
	if err != nil {
		log.Errorf("prepare delete from edgex_deployment %s failed: %v", depID, err)
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(depID)
	if err != nil {
		log.Errorf("delete from edgex_deployment %s failed: %v", depID, err)
		return err
	}

	log.Debugf("deleteDeployment %s succeeded", depID)
	return err
}

// getReadyDeployments() returns array of deployments that are ready to deploy
func getReadyDeployments() (deployments []DataDeployment, err error) {
	return getDeployments("WHERE local_bundle_uri != $1", "")
}

// getUnreadyDeployments() returns array of deployments that are not yet ready to deploy
func getUnreadyDeployments() (deployments []DataDeployment, err error) {
	return getDeployments("WHERE local_bundle_uri = $1", "")
}

// getDeployments() accepts a "WHERE ..." clause and optional parameters and returns the list of deployments
func getDeployments(where string, a ...interface{}) (deployments []DataDeployment, err error) {
	db := getDB()

	var stmt *sql.Stmt
	stmt, err = db.Prepare(`
	SELECT id, bundle_config_id, apid_cluster_id, data_scope_id,
		bundle_config_json, config_json, created, created_by,
		updated, updated_by, bundle_config_name, bundle_uri,
		local_bundle_uri, bundle_checksum, bundle_checksum_type, deploy_status,
		deploy_error_code, deploy_error_message
	FROM edgex_deployment
	` + where)
	if err != nil {
		return
	}
	defer stmt.Close()
	var rows *sql.Rows
	rows, err = stmt.Query(a...)
	if err != nil {
		if err == sql.ErrNoRows {
			return
		}
		log.Errorf("Error querying edgex_deployment: %v", err)
		return
	}
	defer rows.Close()

	deployments = dataDeploymentsFromRows(rows)

	return
}

func dataDeploymentsFromRows(rows *sql.Rows) (deployments []DataDeployment) {
	for rows.Next() {
		dep := DataDeployment{}
		rows.Scan(&dep.ID, &dep.BundleConfigID, &dep.ApidClusterID, &dep.DataScopeID,
			&dep.BundleConfigJSON, &dep.ConfigJSON, &dep.Created, &dep.CreatedBy,
			&dep.Updated, &dep.UpdatedBy, &dep.BundleName, &dep.BundleURI,
			&dep.LocalBundleURI, &dep.BundleChecksum, &dep.BundleChecksumType, &dep.DeployStatus,
			&dep.DeployErrorCode, &dep.DeployErrorMessage,
		)
		deployments = append(deployments, dep)
	}
	return
}

func setDeploymentResults(results apiDeploymentResults) error {

	// also send results to server
	go transmitDeploymentResultsToServer(results)

	log.Debugf("setDeploymentResults: %v", results)

	tx, err := getDB().Begin()
	if err != nil {
		log.Errorf("Unable to begin transaction: %v", err)
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
	UPDATE edgex_deployment
	SET deploy_status=$1, deploy_error_code=$2, deploy_error_message=$3
	WHERE id=$4;
	`)
	if err != nil {
		log.Errorf("prepare updateDeploymentStatus failed: %v", err)
		return err
	}
	defer stmt.Close()

	for _, result := range results {
		res, err := stmt.Exec(result.Status, result.ErrorCode, result.Message, result.ID)
		if err != nil {
			log.Errorf("update edgex_deployment %s to %s failed: %v", result.ID, result.Status, err)
			return err
		}
		n, err := res.RowsAffected()
		if n == 0 || err != nil {
			log.Error(fmt.Sprintf("no deployment matching '%s' to update. skipping.", result.ID))
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Errorf("Unable to commit setDeploymentResults transaction: %v", err)
	}
	return err
}

func updateLocalBundleURI(depID, localBundleUri string) error {

	stmt, err := getDB().Prepare("UPDATE edgex_deployment SET local_bundle_uri=$1 WHERE id=$2;")
	if err != nil {
		log.Errorf("prepare updateLocalBundleURI failed: %v", err)
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(localBundleUri, depID)
	if err != nil {
		log.Errorf("update edgex_deployment %s localBundleUri to %s failed: %v", depID, localBundleUri, err)
		return err
	}

	log.Debugf("update edgex_deployment %s localBundleUri to %s succeeded", depID, localBundleUri)

	return nil
}

func InsertTestDeployment(tx *sql.Tx, dep DataDeployment) error {

	stmt, err := tx.Prepare(`
	INSERT INTO edgex_deployment
		(id, bundle_config_id, apid_cluster_id, data_scope_id,
		bundle_config_json, config_json, created, created_by,
		updated, updated_by, bundle_config_name)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11);
	`)
	if err != nil {
		log.Errorf("prepare insert into edgex_deployment failed: %v", err)
		return err
	}
	defer stmt.Close()

	log.Debugf("InsertTestDeployment: %s", dep.ID)

	_, err = stmt.Exec(
		dep.ID, dep.BundleConfigID, dep.ApidClusterID, dep.DataScopeID,
		dep.BundleConfigJSON, dep.ConfigJSON, dep.Created, dep.CreatedBy,
		dep.Updated, dep.UpdatedBy, dep.BundleName)
	if err != nil {
		log.Errorf("insert into edgex_deployment %s failed: %v", dep.ID, err)
		return err
	}

	log.Debug("InsertTestDeployment edgex_deployment succeeded")
	return err
}
