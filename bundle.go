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

var (
	markDeploymentFailedAfter time.Duration
	bundleDownloadConnTimeout time.Duration
	bundleRetryDelay          = time.Second
	downloadQueue             = make(chan *DownloadRequest, downloadQueueSize)
	workerQueue               = make(chan chan *DownloadRequest, concurrentDownloads)
)

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

func queueDownloadRequest(dep DataDeployment) {

	hashWriter, err := getHashWriter(dep.BundleChecksumType)
	if err != nil {
		msg := fmt.Sprintf("invalid bundle checksum type: %s for deployment: %s", dep.BundleChecksumType, dep.ID)
		log.Error(msg)
		setDeploymentResults(apiDeploymentResults{
			{
				ID:        dep.ID,
				Status:    RESPONSE_STATUS_FAIL,
				ErrorCode: TRACKER_ERR_INVALID_CHECKSUM,
				Message:   msg,
			},
		})
		return
	}

	retryIn := bundleRetryDelay
	maxBackOff := 5 * time.Minute
	markFailedAt := time.Now().Add(markDeploymentFailedAfter)
	req := &DownloadRequest{
		dep:          dep,
		hashWriter:   hashWriter,
		bundleFile:   getBundleFile(dep),
		backoffFunc:  createBackoff(retryIn, maxBackOff),
		markFailedAt: markFailedAt,
	}
	downloadQueue <- req
}

type DownloadRequest struct {
	dep          DataDeployment
	hashWriter   hash.Hash
	bundleFile   string
	backoffFunc  func()
	markFailedAt time.Time
}

func (r *DownloadRequest) downloadBundle() {

	dep := r.dep
	log.Debugf("starting bundle download attempt for %s: %s", dep.ID, dep.BundleURI)

	deployments, err := getDeployments("WHERE id=$1", dep.ID)
	if err == nil && len(deployments) == 0 {
		log.Debugf("never mind, deployment %s was deleted", dep.ID)
		return
	}

	r.checkTimeout()

	r.hashWriter.Reset()
	tempFile, err := downloadFromURI(dep.BundleURI, r.hashWriter, dep.BundleChecksum)

	if err == nil {
		err = os.Rename(tempFile, r.bundleFile)
		if err != nil {
			log.Errorf("Unable to rename temp bundle file %s to %s: %s", tempFile, r.bundleFile, err)
		}
	}

	if tempFile != "" {
		go safeDelete(tempFile)
	}

	if err == nil {
		err = updateLocalBundleURI(dep.ID, r.bundleFile)
	}

	if err != nil {
		// add myself back into the queue after back off
		go func() {
			r.backoffFunc()
			downloadQueue <- r
		}()
		return
	}

	log.Debugf("bundle for %s downloaded: %s", dep.ID, dep.BundleURI)

	// send deployments to client
	deploymentsChanged <- dep.ID
}

func (r *DownloadRequest) checkTimeout() {

	if !r.markFailedAt.IsZero() {
		if time.Now().After(r.markFailedAt) {
			r.markFailedAt = time.Time{}
			log.Debugf("bundle download timeout. marking deployment %s failed. will keep retrying: %s",
				r.dep.ID, r.dep.BundleURI)
			setDeploymentResults(apiDeploymentResults{
				{
					ID:        r.dep.ID,
					Status:    RESPONSE_STATUS_FAIL,
					ErrorCode: TRACKER_ERR_BUNDLE_TIMEOUT,
					Message:   "bundle download failed",
				},
			})
		}
	}
}

func getBundleFile(dep DataDeployment) string {

	// the content of the URI is unfortunately not guaranteed not to change, so I can't just use dep.BundleURI
	// unfortunately, this also means that a bundle cache isn't especially relevant
	fileName := dep.DataScopeID + "_" + dep.ID

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
	client := http.Client{
		Timeout: bundleDownloadConnTimeout,
	}
	res, err := client.Get(uriString)
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

func initializeBundleDownloading() {

	// create workers
	for i := 0; i < concurrentDownloads; i++ {
		worker := BundleDownloader{
			id:       i + 1,
			workChan: make(chan *DownloadRequest),
			quitChan: make(chan bool),
		}
		worker.Start()
	}

	// run dispatcher
	go func() {
		for {
			select {
			case req := <-downloadQueue:
				log.Debugf("dispatching downloader for: %s", req.bundleFile)
				go func() {
					worker := <-workerQueue
					log.Debugf("got a worker for: %s", req.bundleFile)
					worker <- req
				}()
			}
		}
	}()
}

type BundleDownloader struct {
	id       int
	workChan chan *DownloadRequest
	quitChan chan bool
}

func (w *BundleDownloader) Start() {
	go func() {
		log.Debugf("started bundle downloader %d", w.id)
		for {
			// wait for work
			workerQueue <- w.workChan

			select {
			case req := <-w.workChan:
				log.Debugf("starting download %s", req.bundleFile)
				req.downloadBundle()

			case <-w.quitChan:
				log.Debugf("bundle downloader %d stopped", w.id)
				return
			}
		}
	}()
}

func (w *BundleDownloader) Stop() {
	go func() {
		w.quitChan <- true
	}()
}
