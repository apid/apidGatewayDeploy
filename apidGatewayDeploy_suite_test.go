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

	config.Set("data_path", tmpDir)
	config.Set(configBundleDir, tmpDir)

	// init() will create the tables
	apid.InitializePlugins()

	router := apid.API().Router()
	// fake bundle repo
	router.HandleFunc("/bundle", func(w http.ResponseWriter, req *http.Request) {
		w.Write([]byte("bundle stuff"))
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
