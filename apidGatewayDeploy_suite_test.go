package apiGatewayDeploy

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"encoding/hex"
	"github.com/30x/apid-core"
	"github.com/30x/apid-core/factory"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"
)

var (
	tmpDir              string
	testServer          *httptest.Server
	testLastTrackerVars map[string]string
	testLastTrackerBody []byte
)

var _ = BeforeSuite(func() {
	apid.Initialize(factory.DefaultServicesFactory())

	config := apid.Config()

	var err error
	tmpDir, err = ioutil.TempDir("", "api_test")
	Expect(err).NotTo(HaveOccurred())

	config.Set("local_storage_path", tmpDir)
	config.Set(configApidInstanceID, "INSTANCE_ID")
	config.Set(configApidClusterID, "CLUSTER_ID")
	config.Set(configApiServerBaseURI, "http://localhost")

	apid.InitializePlugins()

	db, err := data.DB()
	Expect(err).NotTo(HaveOccurred())
	err = InitDB(db)
	Expect(err).NotTo(HaveOccurred())
	SetDB(db)

	debounceDuration = time.Millisecond
	bundleCleanupDelay = time.Millisecond
	bundleRetryDelay = 10 * time.Millisecond
	bundleDownloadTimeout = 50 * time.Millisecond

	router := apid.API().Router()
	// fake an unreliable bundle repo
	count := 1
	router.HandleFunc("/bundles/{id}", func(w http.ResponseWriter, req *http.Request) {
		count++
		vars := apid.API().Vars(req)
		if count%2 == 0 {
			w.WriteHeader(500)
			return
		}
		if vars["id"] == "longfail" {
			time.Sleep(bundleDownloadTimeout + (250 * time.Millisecond))
		}
		w.Write([]byte("/bundles/" + vars["id"]))

	}).Methods("GET")

	// fake an unreliable APID tracker
	router.HandleFunc("/clusters/{clusterID}/apids/{instanceID}/deployments",
		func(w http.ResponseWriter, req *http.Request) {
			count++
			if count%2 == 0 {
				w.WriteHeader(500)
				return
			}

			testLastTrackerVars = apid.API().Vars(req)
			testLastTrackerBody, err = ioutil.ReadAll(req.Body)
			Expect(err).ToNot(HaveOccurred())

			w.Write([]byte("OK"))

		}).Methods("PUT")
	testServer = httptest.NewServer(router)

	apiServerBaseURI, err = url.Parse(testServer.URL)
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	apid.Events().Close()
	if testServer != nil {
		testServer.Close()
	}
	os.RemoveAll(tmpDir)
})

var _ = BeforeEach(func() {
	_, err := getDB().Exec("DELETE FROM deployments")
	Expect(err).ShouldNot(HaveOccurred())
	_, err = getDB().Exec("UPDATE etag SET value=1")
})

func TestApidGatewayDeploy(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ApidGatewayDeploy Suite")
}

func testGetChecksum(hashType, uri string) string {
	url, err := url.Parse(uri)
	Expect(err).NotTo(HaveOccurred())

	hashWriter, err := getHashWriter(hashType)
	Expect(err).NotTo(HaveOccurred())

	hashWriter.Write([]byte(url.Path))
	return hex.EncodeToString(hashWriter.Sum(nil))
}
