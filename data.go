package apiGatewayDeploy

import (
	"database/sql"
	"github.com/30x/apid"
	"sync"
	"fmt"
)

var (
	unsafeDB apid.DB
	dbMux    sync.RWMutex
)

type DataDeployment struct {
	ID                string
	BundleConfigID    string
	ApidClusterID     string
	DataScopeID       string
	BundleConfigJSON  string
	ConfigJSON  	  string
	Status		  string
	Created		  string
	CreatedBy         string
	Updated           string
	UpdatedBy         string
	BundleName	  string
	BundleURI	  string
	BundleChecksum	  string
	BundleChecksumType string
	LocalBundleURI    string
}

type SQLExec interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

func InitDB(db apid.DB) error {
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS etag (
		value integer
	);
	INSERT INTO etag VALUES (1);
	CREATE TABLE IF NOT EXISTS deployments (
		id character varying(36) NOT NULL,
		bundle_config_id varchar(36) NOT NULL,
		apid_cluster_id varchar(36) NOT NULL,
		data_scope_id varchar(36) NOT NULL,
		bundle_config_json text NOT NULL,
		config_json text NOT NULL,
		status text NOT NULL,
		created timestamp without time zone,
		created_by text,
		updated timestamp without time zone,
		updated_by text,
		bundle_name text,
		bundle_uri text,
		local_bundle_uri text,
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

func SetDB(db apid.DB) {
	dbMux.Lock()
	if unsafeDB == nil { // init API when DB is initialized
		go InitAPI()
	}
	unsafeDB = db
	dbMux.Unlock()
}

// call whenever the list of deployments changes
func incrementETag() error {

	stmt, err := getDB().Prepare("UPDATE etag SET value = value+1;")
	if err != nil {
		log.Errorf("prepare update etag failed: %v", err)
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec()
	if err != nil {
		log.Errorf("update etag failed: %v", err)
		return err
	}

	log.Debugf("etag incremented")
	return err
}

func getETag() (string, error) {

	var eTag string
	db := getDB()
	row := db.QueryRow("SELECT value FROM etag")
	err := row.Scan(&eTag)
	//err := getDB().QueryRow("SELECT value FROM etag").Scan(&eTag)
	if err != nil {
		log.Errorf("select etag failed: %v", err)
		return "", err
	}

	log.Debugf("etag queried: %v", eTag)
	return eTag, err
}

func InsertDeployment(tx *sql.Tx, dep DataDeployment) error {

	log.Debugf("insertDeployment: %s", dep.ID)

	stmt, err := tx.Prepare(`
	INSERT INTO deployments
		(id, bundle_config_id, apid_cluster_id, data_scope_id,
		bundle_config_json, config_json, status, created,
		created_by, updated, updated_by, bundle_name,
		bundle_uri, local_bundle_uri)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14);
	`)
	if err != nil {
		log.Errorf("prepare insert into deployments %s failed: %v", dep.ID, err)
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(
		dep.ID, dep.BundleConfigID, dep.ApidClusterID, dep.DataScopeID,
		dep.BundleConfigJSON, dep.ConfigJSON, dep.Status, dep.Created,
		dep.CreatedBy, dep.Updated, dep.UpdatedBy, dep.BundleName,
		dep.BundleURI, dep.LocalBundleURI)
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

	deploymentsChanged<- depID

	log.Debugf("deleteDeployment %s succeeded", depID)
	return err
}

// getReadyDeployments() returns array of deployments that are ready to deploy
func getReadyDeployments() (deployments []DataDeployment, err error) {

	db := getDB()
	rows, err := db.Query(`
	SELECT id, bundle_config_id, apid_cluster_id, data_scope_id,
		bundle_config_json, config_json, status, created,
		created_by, updated, updated_by, bundle_name,
		bundle_uri, local_bundle_uri
	FROM deployments
	WHERE local_bundle_uri != ""
	`)
	if err != nil {
		if err == sql.ErrNoRows{
			return deployments, nil
		}
		log.Errorf("Error querying deployments: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		dep := DataDeployment{}
		rows.Scan(&dep.ID, &dep.BundleConfigID, &dep.ApidClusterID, &dep.DataScopeID,
			&dep.BundleConfigJSON, &dep.ConfigJSON, &dep.Status, &dep.Created,
			&dep.CreatedBy, &dep.Updated, &dep.UpdatedBy, &dep.BundleName,
			&dep.BundleURI, &dep.LocalBundleURI,
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

func updateLocalURI(depID, localBundleUri string) error {

	tx, err := getDB().Begin()
	if err != nil {
		log.Errorf("begin updateLocalURI failed: %v", err)
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("UPDATE deployments SET local_bundle_uri=$1 WHERE id=$2;")
	if err != nil {
		log.Errorf("prepare updateLocalURI failed: %v", err)
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(localBundleUri, depID)
	if err != nil {
		log.Errorf("update deployments %s localBundleUri to %s failed: %v", depID, localBundleUri, err)
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Errorf("commit updateLocalURI failed: %v", err)
		return err
	}

	log.Debugf("update deployments %s localBundleUri to %s succeeded", depID, localBundleUri)

	return nil
}
