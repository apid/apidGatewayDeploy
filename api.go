package apiGatewayDeploy

import (
	"net/http"
	"database/sql"
	"time"
	"io/ioutil"
	"encoding/json"
	"github.com/30x/apid"
)

// spec: http://playground.apistudio.io/450cdaba-54f0-4ae8-b6b9-11c797418c58/#/

// todo: The following was basically just copied from old APID - needs review.

func distributeEvents() {
	subscribers := make(map[chan string]struct{})
	for {
		select {
		case msg := <-incoming:
			for sub := range subscribers {
				select {
				case sub <- msg:
					log.Info("Handling LP response for devId: ", msg)
				default:
					log.Error("listener too far behind - message dropped")
				}
			}
		case sub := <-addSubscriber:
			log.Info("Add subscriber", sub)
			subscribers[sub] = struct{}{}
		}
	}
}

func handleCurrentDeployment(w http.ResponseWriter, r *http.Request) {

	block := r.URL.Query()["block"] != nil

	/* Retrieve the args from the i/p arg */
	res := sendDeployInfo(w, r)

	/*
	 * If the call has nothing to return, check to see if the call is a
	 * blocking request (Simulate Long Poll)
	 */
	if res == false && block == true {
		log.Info("Blocking request... Waiting for new Deployments.")
		timeout := make(chan bool)
		newReq := make(chan string)

		/* Update channel of a new request (subscriber) */
		addSubscriber <- newReq
		/* Start the timer for the blocking request */
		/* FIXME: 120 sec to be made configurable ? */
		go func() {
			time.Sleep(time.Second * 120)
			timeout <- true
		}()

		select {
		/*
		 * This blocks till either of the two occurs
		 * (1) - Timeout
		 * (2) - A new deployment has occurred
		 * FIXME: <-newReq has the DevId, this can be
		 * used directly instead of getting it via
		 * SQL query in GetDeployInfo()
		 */

		case <-newReq:
			sendDeployInfo(w, r)

		case <-timeout:
			log.Debug("Blocking request Timed out. No new Deployments.")
		}
	}
}

func respHandler(w http.ResponseWriter, r *http.Request) {

	db, err := data.DB()
	if err != nil {
		log.Error("Error accessing database", err)
		return
	}

	// uri is /deployments/{deploymentID}
	depID := apid.API().Vars(r)["deploymentID"]

	if depID == "" {
		log.Error("No deployment ID")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("No deployment ID")) // todo: probably not a valid response per API spec
		return
	}

	var rsp gwBundleResponse
	buf, _ := ioutil.ReadAll(r.Body)
	err = json.Unmarshal(buf, &rsp)
	if err != nil {
		log.Error("Resp Handler Json Unmarshal err: ", err)
		return
	}

	/*
	 * If the state of deployment was success, update state of bundles and
	 * its deployments as success as well
	 */
	txn, err := db.Begin()
	if err != nil {
		log.Error("Unable to begin transaction: ", err)
		return
	}

	var res bool
	if rsp.Status == "SUCCESS" {
		res = updateDeploymentSuccess(depID, txn)
	} else {
		res = updateDeploymentFailure(depID, rsp.GWbunRsp, txn)
	}

	if res == true {
		err = txn.Commit()
	} else {
		err = txn.Rollback()
	}
	if err != nil {
		log.Error("Unable to finish transaction: ", err)
		return
	}

	return

}

func updateDeploymentSuccess(depID string, txn *sql.Tx) bool {

	log.Infof("Marking deployment (%s) as SUCCEEDED", depID)

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
		res, err = txn.Exec("UPDATE BUNDLE_INFO SET deploy_status = ?, errorcode = ?, error_reason = ? WHERE id = ?;", DEPLOYMENT_STATE_ERR_GWY, a.ErrorCode, a.Reason, a.BundleId)
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
