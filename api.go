package apiGatewayDeploy

import (
	"database/sql"
	"encoding/json"
	"github.com/30x/apid"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
	"fmt"
)

const (
	RESPONSE_STATUS_SUCCESS = "SUCCESS"
	RESPONSE_STATUS_FAIL    = "FAIL"

	ERROR_CODE_TODO = 0 // todo: add error codes where this is used
)

var (
	incoming      = make(chan string)
	addSubscriber = make(chan chan string)
)

type errorResponse struct {
	ErrorCode int    `json:"errorCode"`
	Reason    string `json:"reason"`
}

func initAPI(services apid.Services) {
	services.API().HandleFunc("/deployments/current", handleCurrentDeployment).Methods("GET")
	services.API().HandleFunc("/deployments/{deploymentID}", handleDeploymentResult).Methods("POST")
}

func writeError(w http.ResponseWriter, status int, code int, reason string) {
	w.WriteHeader(status)
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
	log.Debugf("sending %d error to client: %s", status, reason)
}

func writeDatabaseError(w http.ResponseWriter) {
	writeError(w, http.StatusInternalServerError, ERROR_CODE_TODO, "database error")
}

func distributeEvents() {
	subscribers := make(map[chan string]struct{})
	for {
		select {
		case msg := <-incoming:
			log.Debugf("Delivering new deployment %s to %d subscribers", msg, len(subscribers))
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

	// If returning without a bundle (immediately or after timeout), status = 404
	// If returning If-None-Match value is equal to current deployment, status = 304
	// If returning a new value, status = 200

	// If timeout > 0 AND there is no deployment (or new deployment) available (per If-None-Match), then
	// block for up to the specified number of seconds until a new deployment becomes available.
	b := r.URL.Query().Get("block")
	var timeout int
	if b != "" {
		var err error
		timeout, err = strconv.Atoi(b)
		if err != nil {
			writeError(w, http.StatusBadRequest, ERROR_CODE_TODO, "bad block value, must be number of seconds")
			return
		}
	}
	log.Debugf("api timeout: %d", timeout)

	// If If-None-Match header matches the ETag of current bundle list AND if the request does NOT have a 'block'
	// query param > 0, the server returns a 304 Not Modified response indicating that the client already has the
	// most recent bundle list.
	priorDepID := r.Header.Get("If-None-Match")
	log.Debugf("if-none-match: %s", priorDepID)

	depID, err := getCurrentDeploymentID()
	if err != nil && err != sql.ErrNoRows{
		writeDatabaseError(w)
		return
	}

	// not found, no timeout, send immediately
	if depID == "" && timeout == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// found, send immediately - if doesn't match prior ID
	if depID != "" {
		if depID == priorDepID {
			if timeout == 0 {
				w.WriteHeader(http.StatusNotModified)
				return
			} else {
				// continue
			}
		} else {
			sendDeployment(w, depID)
			return
		}
	}

	// can't send immediately, we need to block...
	// todo: can we kill the timer & channel if client connection is lost?
	// todo: resolve race condition - may miss a notification

	log.Debug("Blocking request... Waiting for new Deployments.")
	newReq := make(chan string)

	// subscribe to new deployments
	addSubscriber <- newReq

	// block until new deployment or timeout
	select {
	case depID := <-newReq:
		sendDeployment(w, depID)

	case <-time.After(time.Duration(timeout) * time.Second):
		log.Debug("Blocking deployment request timed out.")
		if priorDepID != "" {
			w.WriteHeader(http.StatusNotModified)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
		return
	}
}

func sendDeployment(w http.ResponseWriter, depID string) {
	deployment, err := getDeployment(depID)
	if err != nil {
		log.Errorf("unable to retrieve deployment [%s]: %s", depID, err)
		w.WriteHeader(http.StatusInternalServerError)
	}
	b, err := json.Marshal(deployment)
	if err != nil {
		log.Errorf("unable to marshal deployment: %s", err)
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		log.Debugf("sending deployment %s: %s", depID, b)
		w.Header().Set("ETag", depID)
		w.Write(b)
	}
}

// todo: we'll need to transmit results back to Edge somehow...
func handleDeploymentResult(w http.ResponseWriter, r *http.Request) {

	depID := apid.API().Vars(r)["deploymentID"]

	if depID == "" {
		log.Error("No deployment ID")
		writeError(w, http.StatusBadRequest, ERROR_CODE_TODO, "Missing deployment ID")
		return
	}

	var rsp deploymentResponse
	buf, _ := ioutil.ReadAll(r.Body)
	err := json.Unmarshal(buf, &rsp)
	if err != nil {
		log.Error("Resp Handler Json Unmarshal err: ", err)
		writeError(w, http.StatusBadRequest, ERROR_CODE_TODO, "Malformed JSON")
		return
	}

	if rsp.Status != RESPONSE_STATUS_SUCCESS && rsp.Status != RESPONSE_STATUS_FAIL {
		writeError(w, http.StatusBadRequest, ERROR_CODE_TODO,
			fmt.Sprintf("status must be '%s' or '%s'", RESPONSE_STATUS_SUCCESS, RESPONSE_STATUS_FAIL))
		return
	}

	if rsp.Status == RESPONSE_STATUS_FAIL && (rsp.Error.ErrorCode == 0 || rsp.Error.Reason == "") {
		writeError(w, http.StatusBadRequest, ERROR_CODE_TODO, "errorCode and reason are required")
		return
	}

	err = updateDeploymentAndBundles(depID, rsp)
	if err != nil {
		if err == sql.ErrNoRows {
			writeError(w, http.StatusNotFound, ERROR_CODE_TODO, "not found")
		} else {
			writeDatabaseError(w)
		}
	}

	return
}
