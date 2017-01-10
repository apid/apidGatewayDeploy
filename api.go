package apiGatewayDeploy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

const (
	RESPONSE_STATUS_SUCCESS = "SUCCESS"
	RESPONSE_STATUS_FAIL    = "FAIL"

	// todo: add error codes where this is used
	ERROR_CODE_TODO = 0
)

var (
	deploymentsChanged = make(chan string)
	addSubscriber      = make(chan chan string)
)

type errorResponse struct {
	ErrorCode int    `json:"errorCode"`
	Reason    string `json:"reason"`
}

type apiDeployment struct {
	ID                string `json:"id"`
	ScopeId           string `json:"scopeId"`
	Created           string `json:"created"`
	CreatedBy         string `json:"createdBy"`
	Updated           string `json:"updated"`
	UpdatedBy         string `json:"updatedBy"`
	ConfigurationJson string `json:"configurationJson"`
	DisplayName       string `json:"displayName"`
	URI               string `json:"uri"`
}

// sent to client
type apiDeploymentResponse []apiDeployment

type apiDeploymentResult struct {
	ID        string `json:"id"`
	Status    string `json:"status"`
	ErrorCode int    `json:"errorCode"`
	Message   string `json:"message"`
}

// received from client
type apiDeploymentResults []apiDeploymentResult

const deploymentsEndpoint = "/deployments"

func initAPI() {
	services.API().HandleFunc(deploymentsEndpoint, apiGetCurrentDeployments).Methods("GET")
	services.API().HandleFunc(deploymentsEndpoint, apiSetDeploymentResults).Methods("POST")
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
		case msg := <-deploymentsChanged:
			// todo: add a debounce w/ timeout to avoid sending on every single deployment?
			subs := subscribers
			incrementETag() // todo: do this elsewhere? check error?
			subscribers = make(map[chan string]struct{})
			log.Debugf("Delivering deployment change %s to %d subscribers", msg, len(subs))
			for subscriber := range subs {
				select {
				case subscriber <- msg:
					log.Debugf("Handling deploy response for: %s", msg)
				default:
					log.Debugf("listener too far behind, message dropped")
				}
			}
		case subscriber := <-addSubscriber:
			log.Debugf("Add subscriber: %v", subscriber)
			subscribers[subscriber] = struct{}{}
		}
	}
}

func apiGetCurrentDeployments(w http.ResponseWriter, r *http.Request) {

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
	ifNoneMatch := r.Header.Get("If-None-Match")
	log.Debugf("if-none-match: %s", ifNoneMatch)

	// send unmodified if matches prior eTag and no timeout
	eTag, err := getETag()
	if err != nil {
		writeDatabaseError(w)
		return
	}
	if eTag == ifNoneMatch && timeout == 0 {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// subscribe to new deployments in case we need it
	var gotNewDeployment chan string
	if timeout > 0 && ifNoneMatch != "" {
		gotNewDeployment = make(chan string)
		addSubscriber <- gotNewDeployment
	}

	deployments, err := getReadyDeployments()
	if err != nil {
		writeDatabaseError(w)
		return
	}

	// send not found if no timeout
	if len(deployments) == 0 && timeout == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// send results if different eTag
	if eTag != ifNoneMatch {
		sendDeployments(w, deployments, eTag)
		return
	}

	log.Debug("Blocking request... Waiting for new Deployments.")

	select {
	case <-gotNewDeployment:
		apiGetCurrentDeployments(w, r) // recurse

	case <-time.After(time.Duration(timeout) * time.Second):
		log.Debug("Blocking deployment request timed out.")
		if ifNoneMatch != "" {
			w.WriteHeader(http.StatusNotModified)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}
}

func sendDeployments(w http.ResponseWriter, dataDeps []dataDeployment, eTag string) {

	var apiDeps apiDeploymentResponse

	for _, d := range dataDeps {
		apiDeps = append(apiDeps, apiDeployment{
			ID:                d.ID,
			ScopeId:           d.DataScopeID,
			Created:           d.Created,
			CreatedBy:         d.CreatedBy,
			Updated:           d.Updated,
			UpdatedBy:         d.UpdatedBy,
			ConfigurationJson: d.ConfigJSON,
			DisplayName:       d.BundleName,
			URI:               d.LocalBundleURI,
		})
	}

	b, err := json.Marshal(apiDeps)
	if err != nil {
		log.Errorf("unable to marshal deployments: %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.Debugf("sending deployment %s: %s", eTag, b)
	w.Header().Set("ETag", eTag)
	w.Write(b)
}

func apiSetDeploymentResults(w http.ResponseWriter, r *http.Request) {

	var results apiDeploymentResults
	buf, _ := ioutil.ReadAll(r.Body)
	err := json.Unmarshal(buf, &results)
	if err != nil {
		log.Errorf("Resp Handler Json Unmarshal err: ", err)
		writeError(w, http.StatusBadRequest, ERROR_CODE_TODO, "Malformed JSON")
		return
	}

	// validate the results
	// todo: these errors to the client should be standardized
	var errs bytes.Buffer
	for i, rsp := range results {
		if rsp.ID == "" {
			errs.WriteString(fmt.Sprintf("Missing id at %d\n", i))
		}

		if rsp.Status != RESPONSE_STATUS_SUCCESS && rsp.Status != RESPONSE_STATUS_FAIL {
			errs.WriteString(fmt.Sprintf("status must be '%s' or '%s' at %d\n", RESPONSE_STATUS_SUCCESS, RESPONSE_STATUS_FAIL, i))
		}

		if rsp.Status == RESPONSE_STATUS_FAIL {
			if rsp.ErrorCode == 0 {
				errs.WriteString(fmt.Sprintf("errorCode is required for status == fail at %d\n", i))
			}
			if rsp.Message == "" {
				errs.WriteString(fmt.Sprintf("message are required for status == fail at %d\n", i))
			}
		}
	}
	if errs.Len() > 0 {
		writeError(w, http.StatusBadRequest, ERROR_CODE_TODO, errs.String())
	}

	err = setDeploymentResults(results)
	if err != nil {
		writeDatabaseError(w)
	}

	// todo: transmit to server (API TBD)
	//err = transmitDeploymentResultsToServer()

	return
}
