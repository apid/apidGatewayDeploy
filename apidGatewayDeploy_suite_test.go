package apiGatewayDeploy

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/30x/apid"
	"github.com/30x/apid/factory"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
	"net/url"
	"encoding/hex"
	"os"
)

var (
	tmpDir     string
	testServer *httptest.Server
)

var _ = BeforeSuite(func() {
	apid.Initialize(factory.DefaultServicesFactory())

	config := apid.Config()

	var err error
	tmpDir, err = ioutil.TempDir("", "api_test")
	Expect(err).NotTo(HaveOccurred())

	config.Set("local_storage_path", tmpDir)

	apid.InitializePlugins()

	db, err := data.DB()
	Expect(err).NotTo(HaveOccurred())
	err = InitDB(db)
	Expect(err).NotTo(HaveOccurred())
	SetDB(db)

	router := apid.API().Router()
	// fake an unreliable bundle repo
	backOffMultiplier = 10 * time.Millisecond
	count := 0
	router.HandleFunc("/bundles/{id}", func(w http.ResponseWriter, req *http.Request) {
		count++
		if count % 2 == 0 {
			w.WriteHeader(500)
			return
		}
		vars := apid.API().Vars(req)
		w.Write([]byte("/bundles/" + vars["id"]))
	})
	testServer = httptest.NewServer(router)
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
