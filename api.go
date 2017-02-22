package apiGatewayDeploy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
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
	eTag               int64
)

type errorResponse struct {
	ErrorCode int    `json:"errorCode"`
	Reason    string `json:"reason"`
}

type ApiDeployment struct {
	ID               string          `json:"id"`
	ScopeId          string          `json:"scopeId"`
	Created          string          `json:"created"`
	CreatedBy        string          `json:"createdBy"`
	Updated          string          `json:"updated"`
	UpdatedBy        string          `json:"updatedBy"`
	ConfigJson       json.RawMessage `json:"configuration"`
	BundleConfigJson json.RawMessage `json:"bundleConfiguration"`
	DisplayName      string          `json:"displayName"`
	URI              string          `json:"uri"`
}

// sent to client
type ApiDeploymentResponse []ApiDeployment

type apiDeploymentResult struct {
	ID        string `json:"id"`
	Status    string `json:"status"`
	ErrorCode int    `json:"errorCode"`
	Message   string `json:"message"`
}

// received from client
type apiDeploymentResults []apiDeploymentResult

const deploymentsEndpoint = "/deployments"

func InitAPI() {
	services.API().HandleFunc(deploymentsEndpoint, apiGetCurrentDeployments).Methods("GET")
	services.API().HandleFunc(deploymentsEndpoint, apiSetDeploymentResults).Methods("PUT")
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
	mut := sync.Mutex{}
	msg := ""
	debouncer := func() {
		select {
		case <-time.After(debounceDuration):
			mut.Lock()
			subs := subscribers
			subscribers = make(map[chan string]struct{})
			m := msg
			msg = ""
			incrementETag()
			mut.Unlock()
			log.Debugf("Delivering deployment change %s to %d subscribers", m, len(subs))
			for subscriber := range subs {
				select {
				case subscriber <- m:
					log.Debugf("Handling deploy response for: %s", m)
					log.Debugf("delivering TO: %v", subscriber)
				default:
					log.Debugf("listener too far behind, message dropped")
				}
			}
		}
	}
	for {
		select {
		case newMsg := <-deploymentsChanged:
			mut.Lock()
			log.Debug("deploymentsChanged")
			if msg == "" {
				go debouncer()
			}
			msg = newMsg
			mut.Unlock()
		case subscriber := <-addSubscriber:
			log.Debugf("Add subscriber: %v", subscriber)
			mut.Lock()
			subscribers[subscriber] = struct{}{}
			mut.Unlock()
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
	eTag := getETag()
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

func sendDeployments(w http.ResponseWriter, dataDeps []DataDeployment, eTag string) {

	var apiDeps ApiDeploymentResponse

	for _, d := range dataDeps {
		apiDeps = append(apiDeps, ApiDeployment{
			ID:               d.ID,
			ScopeId:          d.DataScopeID,
			Created:          d.Created,
			CreatedBy:        d.CreatedBy,
			Updated:          d.Updated,
			UpdatedBy:        d.UpdatedBy,
			BundleConfigJson: []byte(d.BundleConfigJSON),
			ConfigJson:       []byte(d.ConfigJSON),
			DisplayName:      d.BundleName,
			URI:              d.LocalBundleURI,
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
	var validResults apiDeploymentResults
	for i, result := range results {
		valid := true
		if result.ID == "" {
			errs.WriteString(fmt.Sprintf("Missing id at %d\n", i))
		}

		if result.Status != RESPONSE_STATUS_SUCCESS && result.Status != RESPONSE_STATUS_FAIL {
			errs.WriteString(fmt.Sprintf("status must be '%s' or '%s' at %d\n",
				RESPONSE_STATUS_SUCCESS, RESPONSE_STATUS_FAIL, i))
		}

		if result.Status == RESPONSE_STATUS_FAIL {
			if result.ErrorCode == 0 {
				errs.WriteString(fmt.Sprintf("errorCode is required for status == fail at %d\n", i))
			}
			if result.Message == "" {
				errs.WriteString(fmt.Sprintf("message are required for status == fail at %d\n", i))
			}
		}

		if valid {
			validResults = append(validResults, result)
		}
	}

	if errs.Len() > 0 {
		writeError(w, http.StatusBadRequest, ERROR_CODE_TODO, errs.String())
		return
	}

	if len(validResults) > 0 {
		go transmitDeploymentResultsToServer(validResults)
		setDeploymentResults(validResults)
	}

	w.Write([]byte("OK"))
}


func addHeaders(req *http.Request) {
	var token = services.Config().GetString("apigeesync_bearer_token")
	req.Header.Add("Authorization", "Bearer "+token)
}


func transmitDeploymentResultsToServer(validResults apiDeploymentResults) error {

	retryIn := bundleRetryDelay
	maxBackOff := 5 * time.Minute
	backOffFunc := createBackoff(retryIn, maxBackOff)

	_, err := url.Parse(apiServerBaseURI.String())
	if err != nil {
		log.Errorf("unable to parse apiServerBaseURI %s: %v", apiServerBaseURI.String(), err)
		return err
	}
	apiPath := fmt.Sprintf("%s/clusters/%s/apids/%s/deployments",apiServerBaseURI.String(), apidClusterID, apidInstanceID)

	resultJSON, err := json.Marshal(validResults)
	if err != nil {
		log.Errorf("unable to marshal deployment results %v: %v", validResults, err)
		return err
	}

	for {
		log.Debugf("transmitting deployment results to tracker by URL=%s data=%s",apiPath, string(resultJSON))
		req, err := http.NewRequest("PUT", apiPath, bytes.NewReader(resultJSON))
		req.Header.Add("Content-Type", "application/json")
		addHeaders(req)

		resp, err := http.DefaultClient.Do(req)
		defer resp.Body.Close();
		if err != nil || resp.StatusCode != http.StatusOK {
			if err != nil {
				log.Errorf("failed to communicate with tracking service: %v", err)
			} else {
				b, _ := ioutil.ReadAll(resp.Body)
				log.Errorf("tracking service call failed to %s , code: %d, body: %s", apiPath, resp.StatusCode, string(b))
			}
			backOffFunc()
			continue
		}
		b, _ := ioutil.ReadAll(resp.Body)
		log.Debugf("tracking service returned %s , code: %d, body: %s", apiPath, resp.StatusCode, string(b))
		return nil
	}
}

// call whenever the list of deployments changes
func incrementETag() {
	atomic.AddInt64(&eTag, 1)
}

func getETag() string {
	e := atomic.LoadInt64(&eTag)
	return strconv.FormatInt(e, 10)
}
