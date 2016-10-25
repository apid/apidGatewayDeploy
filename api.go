package apiGatewayDeploy

import (
	"database/sql"
	"encoding/json"
	"github.com/30x/apid"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

// todo: add error codes where this is used
const ERROR_CODE_TODO = 0

type errorResponse struct {
	ErrorCode int    `json:"errorCode"`
	Reason    string `json:"reason"`
}

func initAPI(services apid.Services) {
	services.API().HandleFunc("/deployments/current", handleCurrentDeployment).Methods("GET")
	services.API().HandleFunc("/deployments/{deploymentID}", respHandler).Methods("POST")
}

func writeError(w http.ResponseWriter, status int, code int, reason string) {
	e := errorResponse{
		ErrorCode: code,
		Reason:    reason,
	}
	bytes, err := json.Marshal(e)
	if err != nil {
		log.Errorf("unable to marshal errorResponse: %v", err)
	} else {
		w.Write(bytes)
	}
	log.Debugf("sending (%d) error to client: %s", status, reason)
	w.WriteHeader(status)
}

func writeDatabaseError(w http.ResponseWriter) {
	writeError(w, http.StatusInternalServerError, ERROR_CODE_TODO, "database error")
}

// todo: The following was basically just copied from old APID - needs review.

func distributeEvents() {
	subscribers := make(map[chan string]struct{})
	for {
		select {
		case msg := <-incoming:
			for subscriber := range subscribers {
				select {
				case subscriber <- msg:
					log.Debugf("Handling deploy response for: %s", msg)
				default:
					log.Error("listener too far behind, message dropped")
				}
			}
		case subscriber := <-addSubscriber:
			log.Debugf("Add subscriber: %s", subscriber)
			subscribers[subscriber] = struct{}{}
		}
	}
}

func handleCurrentDeployment(w http.ResponseWriter, r *http.Request) {

	// If block greater than zero AND if request ETag header not empty AND if there is no new bundle list
	// available, then block for up to the specified number of seconds until a new bundle list becomes
	// available. If no new bundle list becomes available, then return an empty array.
	b := r.URL.Query().Get("block")
	var block int
	if b != "" {
		var err error
		block, err = strconv.Atoi(b)
		if err != nil {
			writeError(w, http.StatusBadRequest, ERROR_CODE_TODO, "bad block value, must be number of seconds")
			return
		}
	}

	sent := sendDeployInfo(w, r, false)

	// todo: can we kill the timer & channel if client connection is lost?

	// blocking request (Long Poll)
	if sent == false && block > 0 && r.Header.Get("etag") != "" {
		log.Debug("Blocking request... Waiting for new Deployments.")
		newReq := make(chan string)

		// Update channel of a new request (subscriber)
		addSubscriber <- newReq

		// Block until timeout of new deployment
		select {
		case depID := <-newReq:
			// todo: depID could be used directly instead of getting it again in sendDeployInfo
			log.Debugf("DeploymentID = %s", depID)
			sendDeployInfo(w, r, false)

		case <-time.After(time.Duration(block) * time.Second):
			log.Debug("Blocking deployment request timed out.")
			sendDeployInfo(w, r, true)
		}
	}
}

func respHandler(w http.ResponseWriter, r *http.Request) {

	depID := apid.API().Vars(r)["deploymentID"]

	if depID == "" {
		log.Error("No deployment ID")
		// todo: add error code
		writeError(w, http.StatusBadRequest, 0, "Missing deployment ID")
		return
	}

	var rsp gwBundleResponse
	buf, _ := ioutil.ReadAll(r.Body)
	err := json.Unmarshal(buf, &rsp)
	if err != nil {
		log.Error("Resp Handler Json Unmarshal err: ", err)
		// todo: add error code
		writeError(w, http.StatusBadRequest, 0, "Malformed body")
		return
	}
	// todo: validate request body

	/*
	 * If the state of deployment was success, update state of bundles and
	 * its deployments as success as well
	 */
	txn, err := db.Begin()
	if err != nil {
		log.Errorf("Unable to begin transaction: %s", err)
		writeDatabaseError(w)
		return
	}

	var updated bool
	if rsp.Status == "SUCCESS" {
		updated = updateDeploymentSuccess(depID, txn)
	} else {
		updated = updateDeploymentFailure(depID, rsp.GWbunRsp, txn)
	}

	log.Print("***** 1")
	if !updated {
		writeDatabaseError(w)
		err = txn.Rollback()
		if err != nil {
			log.Errorf("Unable to rollback transaction: %s", err)
		}
		return
	}

	log.Print("***** 2")
	err = txn.Commit()
	if err != nil {
		log.Errorf("Unable to commit transaction: %s", err)
		writeDatabaseError(w)
	}

	log.Print("***** 3")

	return

}

func updateDeploymentSuccess(depID string, txn *sql.Tx) bool {

	log.Debugf("Marking deployment (%s) as SUCCEEDED", depID)

	var rows int64
	res, err := txn.Exec("UPDATE BUNDLE_INFO SET deploy_status = ? WHERE deployment_id = ?;",
		DEPLOYMENT_STATE_SUCCESS, depID)
	if err == nil {
		rows, err = res.RowsAffected()
	}
	if err != nil || rows == 0 {
		log.Errorf("UPDATE BUNDLE_INFO Failed: Dep Id (%s): %v", depID, err)
		return false
	}

	log.Infof("UPDATE BUNDLE_INFO Success: Dep Id (%s)", depID)

	res, err = txn.Exec("UPDATE BUNDLE_DEPLOYMENT SET deploy_status = ? WHERE id = ?;",
		DEPLOYMENT_STATE_SUCCESS, depID)
	if err != nil {
		rows, err = res.RowsAffected()
	}
	if err != nil || rows == 0 {
		log.Errorf("UPDATE BUNDLE_DEPLOYMENT Failed: Dep Id (%s): %v", depID, err)
		return false
	}

	log.Infof("UPDATE BUNDLE_DEPLOYMENT Success: Dep Id (%s)", depID)

	return true

}

func updateDeploymentFailure(depID string, rsp gwBundleErrorResponse, txn *sql.Tx) bool {

	log.Infof("marking deployment (%s) as FAILED", depID)

	var rows int64
	/* Update the Deployment state errors */
	res, err := txn.Exec("UPDATE BUNDLE_DEPLOYMENT SET deploy_status = ?, error_code = ? WHERE id = ?;",
		DEPLOYMENT_STATE_ERR_GWY, rsp.ErrorCode, depID)
	if err == nil {
		rows, err = res.RowsAffected()
	}
	if err != nil || rows == 0 {
		log.Errorf("UPDATE BUNDLE_DEPLOYMENT Failed: Dep Id (%s): %v", depID, err)
		return false
	}
	log.Infof("UPDATE BUNDLE_DEPLOYMENT Success: Dep Id (%s)", depID)

	/* Iterate over Bundles, and update the errors */
	for _, a := range rsp.ErrorDetails {
		res, err = txn.Exec("UPDATE BUNDLE_INFO SET deploy_status = ?, errorcode = ?, error_reason = ? "+
			"WHERE id = ?;", DEPLOYMENT_STATE_ERR_GWY, a.ErrorCode, a.Reason, a.BundleId)
		if err != nil {
			rows, err = res.RowsAffected()
		}
		if err != nil || rows == 0 {
			log.Errorf("UPDATE BUNDLE_INFO Failed: Bund Id (%s): %v", a.BundleId, err)
			return false
		}
		log.Infof("UPDATE BUNDLE_INFO Success: Bund Id (%s)", a.BundleId)
	}
	return true
}
