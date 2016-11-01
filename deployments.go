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

type bundle struct {
	BundleId string `json:"bundleId"`
	URI      string `json:"uri"`
	AuthCode string `json:"authCode,omitempty"`
}

type deployment struct {
	DeploymentId string   `json:"deploymentId"`
	Bundles      []bundle `json:"bundles"`
	System       bundle   `json:"system"`
}

type deploymentErrorDetail struct {
	ErrorCode int    `json:"errorCode"`
	Reason    string `json:"reason"`
	BundleId  string `json:"bundleId"`
}

type deploymentErrorResponse struct {
	ErrorCode    int                     `json:"errorCode"`
	Reason       string                  `json:"reason"`
	ErrorDetails []deploymentErrorDetail `json:"bundleErrors"`
}

type deploymentResponse struct {
	Status   string                  `json:"status"`
	GWbunRsp deploymentErrorResponse `json:"error"`
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
func prepareBundle(depID string, bun dependantBundle) error {

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
func prepareDeployment(depID string, manifest bundleManifest) error {

	deploymentPath := getDeploymentFilesPath(depID)
	err := os.Mkdir(deploymentPath, 0700)
	if err != nil {
		log.Errorf("Deployment dir creation failed: %v", err)
		return err
	}

	// todo: any reason to put all this in a single transaction?

	err = insertDeployment(depID, manifest)
	if err != nil {
		log.Errorf("Prepare deployment failed: %v", err)
		return err
	}

	// download bundles and store them locally
	errors := make(chan error, len(manifest.DepBun))
	for i, bun := range manifest.DepBun {
		go func() {
			err := prepareBundle(depID, bun)
			errors <- err
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
	for range manifest.DepBun {
		err := <- errors
		if err != nil {
			updateDeploymentStatus(db, depID, DEPLOYMENT_STATE_ERR_APID, ERROR_CODE_TODO)
			return err
		}
	}

	return updateDeploymentStatus(db, depID, DEPLOYMENT_STATE_READY, 0)
}


func serviceDeploymentQueue() {
	log.Debug("Checking for new deployments")

	depID, manifestString := getQueuedDeployment()
	if depID == "" {
		return
	}

	var manifest bundleManifest
	err := json.Unmarshal([]byte(manifestString), &manifest)
	if err != nil {
		log.Errorf("JSON decoding Manifest failed Err: %v", err)
		return
	}

	err = prepareDeployment(depID, manifest)
	if err != nil {
		log.Errorf("Prepare deployment failed: %v", depID)
		return
	}

	err = dequeueDeployment(depID)
	if err != nil {
		log.Warnf("Dequeue deployment failed: %v", depID)
	}

	log.Debugf("Signaling new deployment ready: %s", depID)
	incoming <- depID
}
