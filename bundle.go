package apiGatewayDeploy

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"time"
)

var bundleRetryDelay time.Duration = time.Second
var bundleDownloadTimeout time.Duration = 10 * time.Minute

// simple doubling back-off
func createBackoff(retryIn, maxBackOff time.Duration) func() {
	return func() {
		log.Debugf("backoff called. will retry in %s.", retryIn)
		time.Sleep(retryIn)
		retryIn = retryIn * time.Duration(2)
		if retryIn > maxBackOff {
			retryIn = maxBackOff
		}
	}
}

func downloadBundle(dep DataDeployment) {

	hashWriter, err := getHashWriter(dep.BundleChecksumType)
	if err != nil {
		msg := fmt.Sprintf("invalid bundle checksum type: %s for deployment: %s", dep.BundleChecksumType, dep.ID)
		log.Error(msg)
		setDeploymentResults(apiDeploymentResults{
			{
				ID:        dep.ID,
				Status:    RESPONSE_STATUS_FAIL,
				ErrorCode: ERROR_CODE_TODO,
				Message:   msg,
			},
		})
		return
	}

	log.Debugf("starting bundle download process for %s: %s", dep.ID, dep.BundleURI)

	retryIn := bundleRetryDelay
	maxBackOff := 5 * time.Minute
	backOffFunc := createBackoff(retryIn, maxBackOff)

	// timeout and mark deployment failed
	timeout := time.NewTimer(bundleDownloadTimeout)
	go func() {
		<-timeout.C
		log.Debugf("bundle download timeout. marking deployment %s failed. will keep retrying: %s", dep.ID, dep.BundleURI)
		var errMessage string
		if err != nil {
			errMessage = fmt.Sprintf("bundle download failed: %s", err)
		} else {
			errMessage = "bundle download failed"
		}
		setDeploymentResults(apiDeploymentResults{
			{
				ID:        dep.ID,
				Status:    RESPONSE_STATUS_FAIL,
				ErrorCode: ERROR_CODE_TODO,
				Message:   errMessage,
			},
		})
	}()

	// todo: we'll want to abort download if deployment is deleted
	for {
		var tempFile, bundleFile string
		tempFile, err = downloadFromURI(dep.BundleURI, hashWriter, dep.BundleChecksum)

		if err == nil {
			bundleFile = getBundleFile(dep)
			err = os.Rename(tempFile, bundleFile)
			if err != nil {
				log.Errorf("Unable to rename temp bundle file %s to %s: %s", tempFile, bundleFile, err)
			}
		}

		if tempFile != "" {
			go safeDelete(tempFile)
		}

		if err == nil {
			err = updateLocalBundleURI(dep.ID, bundleFile)
		}

		// success!
		if err == nil {
			break
		}

		backOffFunc()
		hashWriter.Reset()
	}

	log.Debugf("bundle for %s downloaded: %s", dep.ID, dep.BundleURI)

	// send deployments to client
	deploymentsChanged <- dep.ID
}

func getBundleFile(dep DataDeployment) string {

	// the content of the URI is unfortunately not guaranteed not to change, so I can't just use dep.BundleURI
	// unfortunately, this also means that a bundle cache isn't especially relevant
	fileName := dep.DataScopeID + dep.ID + dep.ID

	return path.Join(bundlePath, base64.StdEncoding.EncodeToString([]byte(fileName)))
}

func downloadFromURI(uri string, hashWriter hash.Hash, expectedHash string) (tempFileName string, err error) {

	log.Debugf("Downloading bundle: %s", uri)

	var tempFile *os.File
	tempFile, err = ioutil.TempFile(bundlePath, "download")
	if err != nil {
		log.Errorf("Unable to create temp file: %v", err)
		return
	}
	defer tempFile.Close()
	tempFileName = tempFile.Name()

	var bundleReader io.ReadCloser
	bundleReader, err = getURIFileReader(uri)
	if err != nil {
		log.Errorf("Unable to retrieve bundle %s: %v", uri, err)
		return
	}
	defer bundleReader.Close()

	// track checksum
	teedReader := io.TeeReader(bundleReader, hashWriter)

	_, err = io.Copy(tempFile, teedReader)
	if err != nil {
		log.Errorf("Unable to write bundle %s: %v", tempFileName, err)
		return
	}

	// check checksum
	checksum := hex.EncodeToString(hashWriter.Sum(nil))
	if checksum != expectedHash {
		err = errors.New(fmt.Sprintf("Bad checksum on %s. calculated: %s, given: %s", tempFileName, checksum, expectedHash))
		return
	}

	log.Debugf("Bundle %s downloaded to: %s", uri, tempFileName)
	return
}

// retrieveBundle retrieves bundle data from a URI
func getURIFileReader(uriString string) (io.ReadCloser, error) {

	uri, err := url.Parse(uriString)
	if err != nil {
		return nil, fmt.Errorf("DownloadFileUrl: Failed to parse urlStr: %s", uriString)
	}

	// todo: add authentication - TBD?

	// assume it's a file if no scheme
	if uri.Scheme == "" || uri.Scheme == "file" {
		f, err := os.Open(uri.Path)
		if err != nil {
			return nil, err
		}
		return f, nil
	}

	// GET the contents at uriString
	res, err := http.Get(uriString)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("Bundle uri %s failed with status %d", uriString, res.StatusCode)
	}
	return res.Body, nil
}

func getHashWriter(hashType string) (hash.Hash, error) {

	var hashWriter hash.Hash

	switch hashType {
	case "md5":
		hashWriter = md5.New()
	case "crc-32":
		hashWriter = crc32.NewIEEE()
	default:
		// todo: temporary - this disables checksums until server implements (XAPID-544)
		hashWriter = fakeHash{md5.New()}
		//return nil, errors.New("checksumType must be md5 or crc-32")
	}

	return hashWriter, nil
}

type fakeHash struct {
	hash.Hash
}

func (f fakeHash) Sum(b []byte) []byte {
	return []byte("")
}

//func checksumFile(hashType, checksum string, fileName string) error {
//
//	hashWriter, err := getHashWriter(hashType)
//	if err != nil {
//		return err
//	}
//
//	file, err := os.Open(fileName)
//	if err != nil {
//		return err
//	}
//	defer file.Close()
//
//	if _, err := io.Copy(hashWriter, file); err != nil {
//		return err
//	}
//
//	hashBytes := hashWriter.Sum(nil)
//	//hashBytes := hashWriter.Sum(nil)[:hasher.Size()]
//	//hashBytes := hashWriter.Sum(nil)[:]
//
//	//hex.EncodeToString(hashBytes)
//	if checksum != hex.EncodeToString(hashBytes) {
//		return errors.New(fmt.Sprintf("bad checksum for %s", fileName))
//	}
//
//	return nil
//}
