package apiGatewayDeploy

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/30x/apidApigeeSync" // for direct access to Payload types
	"database/sql"
	"net/http/httptest"
	"github.com/30x/apid"
	"github.com/30x/apid/factory"
	"io/ioutil"
	"net/http"
	"net/url"
	"encoding/json"
	"time"
	"os"
	"strings"
)

const (
	currentDeploymentPath = "/deployments/current"
)

var _ = Describe("api", func() {

	var tmpDir string
	var db *sql.DB
	var server *httptest.Server

	BeforeSuite(func() {
		apid.Initialize(factory.DefaultServicesFactory())

		config := apid.Config()

		var err error
		tmpDir, err = ioutil.TempDir("", "api_test")
		Expect(err).NotTo(HaveOccurred())

		config.Set("data_path", tmpDir)
		config.Set(configBundleDir, tmpDir)

		// init() will create the tables
		apid.InitializePlugins()

		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.URL.Path == currentDeploymentPath {
				handleCurrentDeployment(w, req)
			} else if strings.HasPrefix(req.URL.Path, "/deployments/") {
				respHandler(w, req)
			} else {
				w.Write([]byte("bundle stuff"))
			}
		}))

		db, err = apid.Data().DB()
		Expect(err).NotTo(HaveOccurred())
		insertTestData(server)
	})

	AfterSuite(func() {
		apid.Events().Close()
		if (server != nil) {
			server.Close()
		}
		os.RemoveAll(tmpDir)
	})

	It("should get current deployment", func() {

		uri, err := url.Parse(server.URL)
		uri.Path = currentDeploymentPath

		res, err := http.Get(uri.String())
		defer res.Body.Close()
		Expect(err).ShouldNot(HaveOccurred())

		var depRes deploymentResponse
		body, err := ioutil.ReadAll(res.Body)
		Expect(err).ShouldNot(HaveOccurred())
		json.Unmarshal(body, &depRes)
		Expect(depRes.DeploymentId).Should(Equal("entityID2"))
	})

})

func insertTestData(server *httptest.Server) {

	uri, err := url.Parse(server.URL)
	uri.Path = "/bundle"
	bundleUri := uri.String()

	bundle := bundleManifest{
		systemBundle{
			bundleUri,
		},
		[]dependantBundle{
			{
				bundleUri,
				"someorg",
				"someenv",
			},
		},
	}
	bundleBytes, err := json.Marshal(bundle)
	if err != nil {
		panic("oh crap")
	}

	payload := DataPayload{
		EntityType: "deployment",
		Operation: "create",
		EntityIdentifier: "entityID2",
		PldCont: Payload{
			CreatedAt: time.Now().Unix(),
			Manifest: string(bundleBytes),
		},
	}

	insertDeployment(payload)
}