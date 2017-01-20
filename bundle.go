package apiGatewayDeploy

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"time"
	"encoding/base64"
	"io/ioutil"
	"hash/crc32"
	"errors"
	"crypto/md5"
	"hash"
	"encoding/hex"
)

const (
	DOWNLOAD_ATTEMPTS = 3
)

var (
	backOffMultiplier = 10 * time.Second
)

func downloadBundle(dep DataDeployment) {

	log.Debugf("starting bundle download process: %s", dep.BundleURI)

	hashWriter, err := getHashWriter(dep.BundleChecksumType)
	if err != nil {
		msg := fmt.Sprintf("invalid bundle checksum type: %v", dep.BundleChecksumType)
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

	// todo: do forever with backoff - note: we'll want to abort if deployment is deleted, however
	// todo: also, we'll still mark deployment result as "failed" - after some timeout
	for i := 1; i <= DOWNLOAD_ATTEMPTS; i++ {
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

		if err == nil {
			break
		}

		// simple back-off, we could potentially be more sophisticated
		retryIn := time.Duration(i) * backOffMultiplier
		log.Debugf("will retry failed download in %s: %v", retryIn, err)
		time.Sleep(retryIn)
		hashWriter.Reset()
	}
	if err != nil {
		log.Errorf("failed %d download attempts. aborting.", DOWNLOAD_ATTEMPTS)
	}

	if err != nil {
		setDeploymentResults(apiDeploymentResults{
			{
				ID:        dep.ID,
				Status:    RESPONSE_STATUS_FAIL,
				ErrorCode: ERROR_CODE_TODO,
				Message:   fmt.Sprintf("bundle download failed: %s", err),
			},
		})
		return
	}

	// send deployments to client
	deploymentsChanged<- dep.ID
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
		return nil, errors.New("checksumType must be md5 or crc-32")
	}

	return hashWriter, nil
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
