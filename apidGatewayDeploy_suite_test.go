package apiGatewayDeploy

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/30x/apid"
	"github.com/30x/apid/factory"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
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
	initDB(db)
	setDB(db)

	router := apid.API().Router()
	// fake an unreliable bundle repo
	downloadMultiplier = 10 * time.Millisecond
	count := 0
	router.HandleFunc("/bundle/{id}", func(w http.ResponseWriter, req *http.Request) {
		count++
		if count % 2 == 0 {
			w.WriteHeader(500)
			return
		}
		vars := apid.API().Vars(req)
		w.Write([]byte("/bundle/" + vars["id"]))
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

func TestApidGatewayDeploy(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ApidGatewayDeploy Suite")
}
