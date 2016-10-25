package apiGatewayDeploy

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/30x/apidGatewayDeploy/github"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

// todo: The following was basically just copied from old APID - needs review.

// todo: /current should return latest (regardless of status) if no ETag

/* All Global Constants go here */
const DEPLOYMENT_STATE_UNUSED = 0
const DEPLOYMENT_STATE_INPROG = 1
const DEPLOYMENT_STATE_READY = 2
const DEPLOYMENT_STATE_SUCCESS = 3
const DEPLOYMENT_STATE_ERR_APID = 4
const DEPLOYMENT_STATE_ERR_GWY = 5

var (
	bundlePathAbs     string
	gitHubAccessToken string

	incoming      = make(chan string)
	addSubscriber = make(chan chan string)
)

type systemBundle struct {
	Uri string `json:"uri"`
}

type dependantBundle struct {
	Uri string `json:"uri"`
	Org string `json:"org"`
	Env string `json:"env"`
}

type bundleManifest struct {
	SysBun systemBundle      `json:"system"`
	DepBun []dependantBundle `json:"bundles"`
}

type bundle struct {
	BundleId string `json:"bundleId"`
	URL      string `json:"url"`
	AuthCode string `json:"authCode,omitempty"`
}

type deploymentResponse struct {
	DeploymentId string   `json:"deploymentId"`
	Bundles      []bundle `json:"bundles"`
	System       bundle   `json:"system"`
}

type gwBundleErrorDetail struct {
	ErrorCode int    `json:"errorCode"`
	Reason    string `json:"reason"`
	BundleId  string `json:"bundleId"`
}

type gwBundleErrorResponse struct {
	ErrorCode    int                   `json:"errorCode"`
	Reason       string                `json:"reason"`
	ErrorDetails []gwBundleErrorDetail `json:"bundleErrors"`
}

type gwBundleResponse struct {
	Status   string                `json:"status"`
	GWbunRsp gwBundleErrorResponse `json:"error"`
}

// getBundleResourceData retrieves bundle data from a bundle repo and returns a ReadCloser.
func getBundleResourceData(uriString string) (io.ReadCloser, error) {

	uri, err := url.Parse(uriString)
	if err != nil {
		return nil, fmt.Errorf("DownloadFileUrl: Failed to parse urlStr: %s", uriString)

	}

	// todo: temporary - if not a github url, just open it or call GET on it
	if uri.Host != "github.com" {
		// assume it's a file if no scheme
		if uri.Scheme == "" || uri.Scheme == "file" {
			f, err := os.Open(uri.Path)
			if err != nil {
				return nil, err
			}
			return f, nil
		}

		// some random uri, try to GET it
		res, err := http.Get(uriString)
		if err != nil {
			return nil, err
		}
		return res.Body, nil
	}

	// go get it from github using access token
	return github.GetUrlData(uri, gitHubAccessToken)
}

func createBundle(depPath string, uri string, depid string, org string, env string, typ string, txn *sql.Tx) int {

	status := DEPLOYMENT_STATE_INPROG
	ts := int64(time.Now().UnixNano())
	timeString := strconv.FormatInt(ts, 10)

	var bundleID string
	if typ == "sys" {
		bundleID = typ + "_" + timeString
	} else {
		// todo: stop using org and env
		bundleID = typ + "_" + org + "_" + env + "_" + timeString
	}
	locFile := depPath + "/" + bundleID + ".zip"

	var bundleData io.ReadCloser
	out, err := os.Create(locFile)
	if err != nil {
		log.Error("Unable to create Bundle file ", locFile, " Err: ", err)
		status = DEPLOYMENT_STATE_ERR_APID
		goto FA
	}

	bundleData, err = getBundleResourceData(uri)
	if err != nil {
		log.Error("Unable to read Bundle URI ", uri, " Err: ", err)
		status = DEPLOYMENT_STATE_ERR_APID
		goto FA
	}
	defer bundleData.Close()
	io.Copy(out, bundleData)
	out.Close()

FA:
	locFile = "file://" + locFile
	success := createInitBundleDB(locFile, bundleID, ts, env, org, depid,
		typ, locFile, status, txn)

	if !success {
		return -1
	} else if status == DEPLOYMENT_STATE_ERR_APID {
		return 1
	} else {
		return 0
	}
}

func orchestrateDeploymentAndTrigger() {

	depId := orchestrateDeployment()
	if depId != "" {
		incoming <- depId
	}
}

/*
 * The series of actions to be taken here are :-
 * (1) Find the latest Deployment id that is in Init state
 * (2) Parse the Manifest URL
 * (3) Download the system bundle and store locally, update DB
 * (4) Download the dependent bundle and store locally, update DB
 * (5) Update deployment state based on the status of deployment
 // returns deploymentID
*/
func orchestrateDeployment() string {

	/* (1) Find the latest deployment, if none - get out */
	status := DEPLOYMENT_STATE_READY
	txn, _ := db.Begin()

	var manifestString, deploymentID string
	err := db.QueryRow("SELECT id, manifest FROM BUNDLE_DEPLOYMENT WHERE deploy_status = ? "+
		"ORDER BY created_at ASC LIMIT 1;", DEPLOYMENT_STATE_UNUSED).
		Scan(&deploymentID, &manifestString)

	switch {
	case err == sql.ErrNoRows:
		log.Error("No Deployments available to be processed")
		return ""
	case err != nil:
		log.Error("SELECT on BUNDLE_DEPLOYMENT failed with Err: ", err)
		return ""
	}

	/* (2) Parse Manifest  */
	var bf bundleManifest
	var fileInfo os.FileInfo
	var deploymentPath string
	var res int
	var result bool
	err = json.Unmarshal([]byte(manifestString), &bf)
	if err != nil {
		log.Error("JSON decoding Manifest failed Err: ", err)
		status = DEPLOYMENT_STATE_ERR_APID
		goto EB
	}

	// todo: validate bundle!
	//for bun := range bf.DepBun {
	//	if bun.uri
	//}

	fileInfo, err = os.Stat(bundlePathAbs)
	if err != nil || !fileInfo.IsDir() {
		log.Error("Path ", bundlePathAbs, " is not a directory")
		status = DEPLOYMENT_STATE_ERR_APID
		goto EB
	}

	deploymentPath = bundlePathAbs + "/" + deploymentID
	err = os.Mkdir(deploymentPath, 0700)
	if err != nil {
		log.Errorf("Deployment Dir creation error: %v", err)
		status = DEPLOYMENT_STATE_ERR_APID
		goto EB
	}

	/* (3) Download system bundle and store locally */
	res = createBundle(deploymentPath, bf.SysBun.Uri, deploymentID, "", "", "sys", txn)
	if res == -1 {
		log.Error("Abort Txn: Unable to update DB with system bundle info")
		goto EC
	} else if res == 1 {
		status = DEPLOYMENT_STATE_ERR_APID
	}

	/* (4) Loop through the Dependent bundles and store them locally as well */
	for _, ele := range bf.DepBun {
		res = createBundle(deploymentPath, ele.Uri, deploymentID, ele.Org, ele.Env, "dep", txn)
		if res == -1 {
			log.Error("Abort Txn: Unable to update DB with dependent bundle info")
			goto EC
		} else if res == 1 {
			status = DEPLOYMENT_STATE_ERR_APID
		}
	}
EB:
	if status == DEPLOYMENT_STATE_ERR_APID && deploymentPath != "" {
		os.RemoveAll(deploymentPath)
	}
	/* (5) Update Deployment state accordingly */
	result = updateDeployStatusDB(deploymentID, status, txn)
	if result == false {
		log.Error("Abort Txn: Unable to update DB with Deployment status")
		goto EC
	}
	txn.Commit()
	return deploymentID
EC:
	os.RemoveAll(deploymentPath)
	txn.Rollback()
	return ""
}

/*
 * Create Init Bundle (FIXME : Put this in a struct and pass - too many i/p args)
 */
func createInitBundleDB(fileurl string, id string, cts int64, env string, org string, depid string, typ string, loc string, status int, txn *sql.Tx) bool {

	_, err := txn.Exec("INSERT INTO BUNDLE_INFO (id, deployment_id, org, env, url, type, deploy_status, "+
		"created_at, file_url)VALUES(?,?,?,?,?,?,?,?,?);", id, depid, org, env, loc, typ, status, cts, fileurl)

	if err != nil {
		log.Error("INSERT BUNDLE_INFO Failed (id, dep id) : (", id, ", ", depid, ")", err)
		return false
	} else {
		log.Info("INSERT BUNDLE_INFO Success: (", id, ")")
		return true
	}

}

func updateDeployStatusDB(id string, status int, txn *sql.Tx) bool {

	_, err := txn.Exec("UPDATE BUNDLE_INFO SET deploy_status = ? WHERE deployment_id = ?;", status, id)
	if err != nil {
		log.Error("UPDATE BUNDLE_INFO Failed: (", id, ") : ", err)
		return false
	} else {
		log.Info("UPDATE BUNDLE_INFO Success: (", id, ")")
	}

	_, err = txn.Exec("UPDATE BUNDLE_DEPLOYMENT SET deploy_status = ? WHERE id = ?;", status, id)
	if err != nil {
		log.Error("UPDATE BUNDLE_DEPLOYMENT Failed: (", id, ") : ", err)
		return false
	} else {
		log.Info("UPDATE BUNDLE_DEPLOYMENT Success: (", id, ")")
	}
	return true

}

func sendDeployInfo(w http.ResponseWriter, r *http.Request, sendEmpty bool) bool {

	// If If-None-Match header matches the ETag of current bundle list AND if the request does NOT have a 'block'
	// query param > 0, the server returns a 304 Not Modified response indicating that the client already has the
	// most recent bundle list.
	ifNoneMatch := r.Header.Get("If-None-Match")

	// Pick the most recent deployment
	var depID string
	// todo: fix /current
	err := db.QueryRow("SELECT id FROM BUNDLE_DEPLOYMENT WHERE deploy_status = ? ORDER BY created_at ASC LIMIT 1;",
		DEPLOYMENT_STATE_READY).Scan(&depID)
	//err = db.QueryRow("SELECT id FROM BUNDLE_DEPLOYMENT ORDER BY created_at ASC LIMIT 1;").Scan(&depID)
	if err != nil {
		log.Errorf("Database error: %s", err)
		return false
	}

	// todo: is depID appropriate for eTag?
	if depID == ifNoneMatch {
		w.WriteHeader(http.StatusNotModified)
		return true
	}
	w.Header().Set("ETag", depID)

	var bundleID, fileUrl string
	// todo: fix /current
	err = db.QueryRow("SELECT file_url, id FROM BUNDLE_INFO WHERE deploy_status = ? AND deployment_id = ? AND "+
		"type = 'sys';", DEPLOYMENT_STATE_READY, depID).Scan(&fileUrl, &bundleID)
	//err = db.QueryRow("SELECT file_url, id FROM BUNDLE_INFO WHERE deployment_id = ? AND " +
	//	"type = 'sys';", DEPLOYMENT_STATE_READY, depID).Scan(&fileUrl, &bundleID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Debugf("No System Deployment ready: %s", err)
			if !sendEmpty {
				return false
			}
		} else {
			log.Errorf("Database error: %s", err)
			return false
		}
	}

	chItems := []bundle{}

	sysInfo := bundle{
		BundleId: bundleID,
		URL:      fileUrl,
	}

	depResp := deploymentResponse{
		Bundles:      chItems,
		DeploymentId: depID,
		System:       sysInfo,
	}

	// todo: fix /current
	rows, err := db.Query("SELECT file_url, id FROM BUNDLE_INFO WHERE deploy_status = ? AND deployment_id = ? "+
		"AND type = 'dep';", DEPLOYMENT_STATE_READY, depID)
	//rows, err := db.Query("SELECT file_url, id FROM BUNDLE_INFO WHERE deployment_id = ? " +
	//	"AND type = 'dep';", depID)
	if err != nil {
		log.Debugf("No Deployments ready: %s", err)
		if !sendEmpty {
			return false
		}
	}
	for rows.Next() {
		err = rows.Scan(&fileUrl, &bundleID)
		if err != nil {
			log.Errorf("Deployments fetch failed. Err: %s", err)
			return false
		}
		bd := bundle{
			AuthCode: bundleID, // todo: authCode
			BundleId: bundleID,
			URL:      fileUrl,
		}
		depResp.Bundles = append(depResp.Bundles, bd)
	}
	b, err := json.Marshal(depResp)
	w.Write(b)
	return true
}
