package apiGatewayDeploy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"
)

// todo: the full set of states should probably be RECEIVED, READY, FAIL, SUCCESS
const (
	RESPONSE_STATUS_SUCCESS = "SUCCESS"
	RESPONSE_STATUS_FAIL    = "FAIL"
)

const (
	TRACKER_ERR_BUNDLE_DOWNLOAD_TIMEOUT = iota + 1
	TRACKER_ERR_BUNDLE_BAD_CHECKSUM
	TRACKER_ERR_DEPLOYMENT_BAD_JSON
)

const (
	API_ERR_BAD_BLOCK = iota + 1
	API_ERR_BAD_JSON
	API_ERR_BAD_CONTENT
	API_ERR_INTERNAL
)

const (
	sqlTimeFormat    = "2006-01-02 15:04:05.999 -0700 MST"
	iso8601          = "2006-01-02T15:04:05.999Z07:00"
	sqliteTimeFormat = "2006-01-02 15:04:05.999-07:00"
)

type deploymentsResult struct {
	deployments []DataDeployment
	err         error
	eTag        string
}

var (
	deploymentsChanged = make(chan interface{}, 5)
	addSubscriber      = make(chan chan deploymentsResult)
	removeSubscriber   = make(chan chan deploymentsResult)
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
	writeError(w, http.StatusInternalServerError, API_ERR_INTERNAL, "database error")
}

func debounce(in chan interface{}, out chan []interface{}, window time.Duration) {
	send := func(toSend []interface{}) {
		if toSend != nil {
			log.Debugf("debouncer sending: %v", toSend)
			out <- toSend
		}
	}
	var toSend []interface{}
	for {
		select {
		case incoming, ok := <-in:
			if ok {
				log.Debugf("debouncing %v", incoming)
				toSend = append(toSend, incoming)
			} else {
				send(toSend)
				log.Debugf("closing debouncer")
				close(out)
				return
			}
		case <-time.After(window):
			send(toSend)
			toSend = nil
		}
	}
}

func distributeEvents() {
	subscribers := make(map[chan deploymentsResult]struct{})
	deliverDeployments := make(chan []interface{}, 1)

	go debounce(deploymentsChanged, deliverDeployments, debounceDuration)

	for {
		select {
		case _, ok := <-deliverDeployments:
			if !ok {
				return // todo: using this?
			}
			subs := subscribers
			subscribers = make(map[chan deploymentsResult]struct{})
			go func() {
				eTag := incrementETag()
				deployments, err := getReadyDeployments()
				log.Debugf("delivering deployments to %d subscribers", len(subs))
				for subscriber := range subs {
					log.Debugf("delivering to: %v", subscriber)
					subscriber <- deploymentsResult{deployments, err, eTag}
				}
			}()
		case subscriber := <-addSubscriber:
			log.Debugf("Add subscriber: %v", subscriber)
			subscribers[subscriber] = struct{}{}
		case subscriber := <-removeSubscriber:
			log.Debugf("Remove subscriber: %v", subscriber)
			delete(subscribers, subscriber)
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
			writeError(w, http.StatusBadRequest, API_ERR_BAD_BLOCK, "bad block value, must be number of seconds")
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

	// send results if different eTag
	if eTag != ifNoneMatch {
		sendReadyDeployments(w)
		return
	}

	// otherwise, subscribe to any new deployment changes
	var newDeploymentsChannel chan deploymentsResult
	if timeout > 0 && ifNoneMatch != "" {
		newDeploymentsChannel = make(chan deploymentsResult, 1)
		addSubscriber <- newDeploymentsChannel
	}

	log.Debug("Blocking request... Waiting for new Deployments.")

	select {
	case result := <-newDeploymentsChannel:
		if result.err != nil {
			writeDatabaseError(w)
		} else {
			sendDeployments(w, result.deployments, result.eTag)
		}

	case <-time.After(time.Duration(timeout) * time.Second):
		removeSubscriber <- newDeploymentsChannel
		log.Debug("Blocking deployment request timed out.")
		if ifNoneMatch != "" {
			w.WriteHeader(http.StatusNotModified)
		} else {
			sendReadyDeployments(w)
		}
	}
}

func sendReadyDeployments(w http.ResponseWriter) {
	eTag := getETag()
	deployments, err := getReadyDeployments()
	if err != nil {
		writeDatabaseError(w)
		return
	}
	sendDeployments(w, deployments, eTag)
}

func sendDeployments(w http.ResponseWriter, dataDeps []DataDeployment, eTag string) {

	apiDeps := ApiDeploymentResponse{}

	for _, d := range dataDeps {
		apiDeps = append(apiDeps, ApiDeployment{
			ID:               d.ID,
			ScopeId:          d.DataScopeID,
			Created:          convertTime(d.Created),
			CreatedBy:        d.CreatedBy,
			Updated:          convertTime(d.Updated),
			UpdatedBy:        d.UpdatedBy,
			BundleConfigJson: []byte(d.BundleConfigJSON),
			ConfigJson:       []byte(d.ConfigJSON),
			DisplayName:      d.BundleName,
			URI:              d.LocalBundleURI,
		})
	}

	b, err := json.Marshal(apiDeps)
	if err != nil {
		log.Errorf("unable to marshal deployments: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.Debugf("sending deployments %s: %s", eTag, b)
	w.Header().Set("ETag", eTag)
	w.Write(b)
}

func apiSetDeploymentResults(w http.ResponseWriter, r *http.Request) {

	var results apiDeploymentResults
	buf, _ := ioutil.ReadAll(r.Body)
	err := json.Unmarshal(buf, &results)
	if err != nil {
		log.Errorf("Resp Handler Json Unmarshal err: ", err)
		writeError(w, http.StatusBadRequest, API_ERR_BAD_JSON, "Malformed JSON")
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
		writeError(w, http.StatusBadRequest, API_ERR_BAD_CONTENT, errs.String())
		return
	}

	if len(validResults) > 0 {
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
	apiPath := fmt.Sprintf("%s/clusters/%s/apids/%s/deployments", apiServerBaseURI.String(), apidClusterID, apidInstanceID)

	resultJSON, err := json.Marshal(validResults)
	if err != nil {
		log.Errorf("unable to marshal deployment results %v: %v", validResults, err)
		return err
	}

	for {
		log.Debugf("transmitting deployment results to tracker by URL=%s data=%s", apiPath, string(resultJSON))
		req, err := http.NewRequest("PUT", apiPath, bytes.NewReader(resultJSON))
		if err != nil {
			log.Errorf("unable to create PUT request", err)
			return err
		}
		req.Header.Add("Content-Type", "application/json")
		addHeaders(req)

		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			if err != nil {
				log.Errorf("failed to communicate with tracking service: %v", err)
			} else {
				b, _ := ioutil.ReadAll(resp.Body)
				log.Errorf("tracking service call failed to %s, code: %d, body: %s", apiPath, resp.StatusCode, string(b))
			}
			resp.Body.Close()
			backOffFunc()
			continue
		}
		resp.Body.Close()
		return nil
	}
}

// call whenever the list of deployments changes
func incrementETag() string {
	e := atomic.AddInt64(&eTag, 1)
	return strconv.FormatInt(e, 10)
}

func getETag() string {
	e := atomic.LoadInt64(&eTag)
	return strconv.FormatInt(e, 10)
}

func convertTime(t string) string {
	if t == "" {
		return ""
	}
	formats := []string{sqliteTimeFormat, sqlTimeFormat, iso8601, time.RFC3339}
	for _, f := range formats {
		timestamp, err := time.Parse(f, t)
		if err == nil {
			return timestamp.Format(iso8601)
		}
	}
	log.Panic("convertTime: Unsupported time format: " + t)
	return ""
}
