package apiGatewayDeploy

import (
	"encoding/json"
	"fmt"
	"github.com/30x/apidGatewayDeploy/github"
	"io"
	"net/http"
	"net/url"
	"os"
	"encoding/base64"
	"path"
	"errors"
	"sync"
)

var (
	bundlePath string
	gitHubAccessToken string // todo: temporary - should come from Manifest
)

type systemBundle struct {
	URI string `json:"uri"`
}

type dependantBundle struct {
	URI   string `json:"uri"`
	Scope string `json:"scope"`
	Org string   `json:"org"`
	Env string   `json:"env"`
}

type bundleManifest struct {
	SysBun systemBundle      `json:"system"`
	DepBun []dependantBundle `json:"bundles"`
}

// event bundle
type bundle struct {
	BundleID string  `json:"bundleId"`
	URI      string  `json:"uri"`
	Scope    string  `json:"scope"`
	Org      string  `json:"org"`
	Env      string  `json:"env"`
}

// event deployment
type deployment struct {
	DeploymentID string   `json:"deploymentId"`
	System       bundle   `json:"system"`
	Bundles      []bundle `json:"bundles"`
}

type deploymentErrorDetail struct {
	ErrorCode int    `json:"errorCode"`
	Reason    string `json:"reason"`
	BundleID  string `json:"bundleId"`
}

type deploymentErrorResponse struct {
	ErrorCode    int                     `json:"errorCode"`
	Reason       string                  `json:"reason"`
	ErrorDetails []deploymentErrorDetail `json:"bundleErrors"`
}

type deploymentResponse struct {
	Status string                  `json:"status"`
	Error  deploymentErrorResponse `json:"error"`
}

// retrieveBundle retrieves bundle data from a URI
func retrieveBundle(uriString string) (io.ReadCloser, error) {

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

// todo: retry on error?
// check if already exists and skip
func prepareBundle(depID string, bun bundle) error {

	bundleFile := getBundleFilePath(depID, bun.URI)
	out, err := os.Create(bundleFile)
	if err != nil {
		log.Errorf("Unable to create bundle file %s, Err: %s", bundleFile, err)
		return err
	}
	defer out.Close()

	bundleData, err := retrieveBundle(bun.URI)
	if err != nil {
		log.Errorf("Unable to retrieve bundle %s, Err: %s", bun.URI, err)
		return err
	}
	defer bundleData.Close()

	_, err = io.Copy(out, bundleData)
	if err != nil {
		log.Errorf("Unable to write bundle %s, Err: %s", bundleFile, err)
		return err
	}

	return nil
}

func getDeploymentFilesPath(depID string) string {
	return bundlePath + "/" + depID
}

func getBundleFilePath(depID string, bundleURI string) string {
	return path.Join(getDeploymentFilesPath(depID), base64.StdEncoding.EncodeToString([]byte(bundleURI)))
}

// returns first bundle download error
// all bundles will be attempted regardless of errors, in the future we could retry
func prepareDeployment(depID string, dep deployment) error {

	log.Debugf("preparing deployment: %s", depID)

	err := insertDeployment(depID, dep)
	if err != nil {
		log.Errorf("insert deployment failed: %v", err)
		return err
	}

	deploymentPath := getDeploymentFilesPath(depID)
	err = os.Mkdir(deploymentPath, 0700)
	if err != nil {
		log.Errorf("Deployment dir creation failed: %v", err)
		return err
	}

	// download bundles and store them locally
	errorsChan := make(chan error, len(dep.Bundles))
	for i, bun := range dep.Bundles {
		go func() {
			err := prepareBundle(depID, bun)
			errorsChan <- err
			if err != nil {
				id := string(i)
				err = updateBundleStatus(db, depID, id, DEPLOYMENT_STATE_ERR_APID, ERROR_CODE_TODO, err.Error())
				if err != nil {
					log.Errorf("Update bundle %s:%s status failed: %v", depID, id, err)
				}
			}
		}()
	}

	// fail fast on first error, otherwise wait for completion
	for range dep.Bundles {
		err := <-errorsChan
		if err != nil {
			updateDeploymentStatus(db, depID, DEPLOYMENT_STATE_ERR_APID, ERROR_CODE_TODO)
			return err
		}
	}

	return updateDeploymentStatus(db, depID, DEPLOYMENT_STATE_READY, 0)
}

var queueMutex sync.Mutex

func serviceDeploymentQueue() {

	queueMutex.Lock()
	defer queueMutex.Unlock()

	log.Debug("Checking for new deployments")

	// todo: this does a get+delete - could lead to a missing deployment if there's a failure
	depID, manifestString := getQueuedDeployment()
	if depID == "" {
		return
	}

	manifest, err := parseManifest(manifestString)
	if err != nil {
		return
	}

	err = prepareDeployment(depID, manifest)
	if err != nil {
		log.Errorf("serviceDeploymentQueue prepare deployment failed: %v", depID)
		return
	}

	deleteDeploymentFromQueue(depID)

	log.Debugf("Signaling new deployment ready: %s", depID)
	incoming <- depID
}

func parseManifest(manifestString string) (dep deployment, err error) {
	err = json.Unmarshal([]byte(manifestString), &dep)
	if err != nil {
		log.Errorf("JSON decoding Manifest failed Err: %v", err)
		return
	}

	// validate manifest
	if dep.System.URI == "" {
		err = errors.New("system bundle 'uri' is required")
		return
	}
	for _, bun := range dep.Bundles {
		if bun.BundleID == "" {
			err = errors.New("bundle 'bundleId' is required")
			return
		}
		if bun.URI == "" {
			err = errors.New("bundle 'uri' is required")
			return
		}
		if bun.Scope == "" {
			err = errors.New("bundle 'scope' is required")
			return
		}
	}

	return
}
