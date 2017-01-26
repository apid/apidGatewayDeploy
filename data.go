package apiGatewayDeploy

import (
	"database/sql"
	"fmt"
	"github.com/30x/apid"
	"sync"
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

func InitDB(db apid.DB) error {
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS deployments (
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
		bundle_name text,
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

	log.Debugf("insertDeployment: %s", dep.ID)

	stmt, err := tx.Prepare(`
	INSERT INTO deployments
		(id, bundle_config_id, apid_cluster_id, data_scope_id,
		bundle_config_json, config_json, created, created_by,
		updated, updated_by, bundle_name, bundle_uri, local_bundle_uri,
		bundle_checksum, bundle_checksum_type, deploy_status,
		deploy_error_code, deploy_error_message)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18);
	`)
	if err != nil {
		log.Errorf("prepare insert into deployments %s failed: %v", dep.ID, err)
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(
		dep.ID, dep.BundleConfigID, dep.ApidClusterID, dep.DataScopeID,
		dep.BundleConfigJSON, dep.ConfigJSON, dep.Created, dep.CreatedBy,
		dep.Updated, dep.UpdatedBy, dep.BundleName, dep.BundleURI,
		dep.LocalBundleURI, dep.BundleChecksum, dep.BundleChecksumType, dep.DeployStatus,
		dep.DeployErrorCode, dep.DeployErrorMessage)
	if err != nil {
		log.Errorf("insert into deployments %s failed: %v", dep.ID, err)
		return err
	}

	log.Debugf("insert into deployments %s succeeded", dep.ID)
	return err
}

func deleteDeployment(tx *sql.Tx, depID string) error {

	log.Debugf("deleteDeployment: %s", depID)

	stmt, err := tx.Prepare("DELETE FROM deployments where id = $1;")
	if err != nil {
		log.Errorf("prepare delete from deployments %s failed: %v", depID, err)
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(depID)
	if err != nil {
		log.Errorf("delete from deployments %s failed: %v", depID, err)
		return err
	}

	deploymentsChanged <- depID

	log.Debugf("deleteDeployment %s succeeded", depID)
	return err
}

// getReadyDeployments() returns array of deployments that are ready to deploy
func getReadyDeployments() (deployments []DataDeployment, err error) {
	return getDeployments("WHERE local_bundle_uri != $1", "")
}

// getUnreadyDeployments() returns array of deployments that are not yet ready to deploy
func getUnreadyDeployments() (deployments []DataDeployment, err error) {
	return getDeployments("WHERE local_bundle_uri = $1 and deploy_status = $2", "", "")
}

// getDeployments() accepts a "WHERE ..." clause and optional parameters and returns the list of deployments
func getDeployments(where string, a ...interface{}) (deployments []DataDeployment, err error) {
	db := getDB()

	var stmt *sql.Stmt
	stmt, err = db.Prepare(`
	SELECT id, bundle_config_id, apid_cluster_id, data_scope_id,
		bundle_config_json, config_json, created, created_by,
		updated, updated_by, bundle_name, bundle_uri,
		local_bundle_uri, bundle_checksum, bundle_checksum_type, deploy_status,
		deploy_error_code, deploy_error_message
	FROM deployments
	` + where)
	if err != nil {
		return
	}
	var rows *sql.Rows
	rows, err = stmt.Query(a...)
	if err != nil {
		if err == sql.ErrNoRows {
			return
		}
		log.Errorf("Error querying deployments: %v", err)
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

	log.Debugf("setDeploymentResults: %v", results)

	tx, err := getDB().Begin()
	if err != nil {
		log.Errorf("Unable to begin transaction: %v", err)
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
	UPDATE deployments
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
			log.Errorf("update deployments %s to %s failed: %v", result.ID, result.Status, err)
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

	stmt, err := getDB().Prepare("UPDATE deployments SET local_bundle_uri=$1 WHERE id=$2;")
	if err != nil {
		log.Errorf("prepare updateLocalBundleURI failed: %v", err)
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(localBundleUri, depID)
	if err != nil {
		log.Errorf("update deployments %s localBundleUri to %s failed: %v", depID, localBundleUri, err)
		return err
	}

	log.Debugf("update deployments %s localBundleUri to %s succeeded", depID, localBundleUri)

	return nil
}

func getLocalBundleURI(tx *sql.Tx, depID string) (localBundleUri string, err error) {

	err = tx.QueryRow("SELECT local_bundle_uri FROM deployments WHERE id=$1;", depID).Scan(&localBundleUri)
	if err == sql.ErrNoRows {
		err = nil
	}
	return
}
