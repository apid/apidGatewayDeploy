package apiGatewayDeploy

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
	"github.com/30x/apid"
	"github.com/30x/apid/factory"
	"io/ioutil"
	"net/http/httptest"
	"os"
	"net/http"
)

var (
	tmpDir string
	testServer *httptest.Server
)

var _ = BeforeSuite(func() {
	apid.Initialize(factory.DefaultServicesFactory())

	config := apid.Config()

	// todo: This will change after apidApigeeSync is fixed for scopes
	config.SetDefault("apigeesync_proxy_server_base", "X")
	config.SetDefault("apigeesync_organization", "X")
	config.SetDefault("apigeesync_consumer_key", "X")
	config.SetDefault("apigeesync_consumer_secret", "X")

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
	if (testServer != nil) {
		testServer.Close()
	}
	os.RemoveAll(tmpDir)
})

func TestApidGatewayDeploy(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ApidGatewayDeploy Suite")
}
