package apiGatewayDeploy

import (
	"fmt"
	"github.com/30x/apid-core"
	"net/url"
	"os"
	"path"
	"time"
)

const (
	configBundleDirKey          = "gatewaydeploy_bundle_dir"
	configDebounceDuration      = "gatewaydeploy_debounce_duration"
	configBundleCleanupDelay    = "gatewaydeploy_bundle_cleanup_delay"
	configBundleDownloadTimeout = "gatewaydeploy_bundle_download_timeout"
	configApiServerBaseURI      = "apigeesync_proxy_server_base"
	configApidInstanceID        = "apigeesync_apid_instance_id"
	configApidClusterID         = "apigeesync_cluster_id"
)

var (
	services           apid.Services
	log                apid.LogService
	data               apid.DataService
	bundlePath         string
	debounceDuration   time.Duration
	bundleCleanupDelay time.Duration
	apiServerBaseURI   *url.URL
	apidInstanceID     string
	apidClusterID      string
)

func init() {
	apid.RegisterPlugin(initPlugin)
}

func initPlugin(s apid.Services) (apid.PluginData, error) {
	services = s
	log = services.Log().ForModule("apiGatewayDeploy")
	log.Debug("start init")

	config := services.Config()

	if !config.IsSet(configApiServerBaseURI) {
		return pluginData, fmt.Errorf("Missing required config value: %s", configApiServerBaseURI)
	}
	var err error
	apiServerBaseURI, err = url.Parse(config.GetString(configApiServerBaseURI))
	if err != nil {
		return pluginData, fmt.Errorf("%s value %s parse err: %v", configApiServerBaseURI, apiServerBaseURI, err)
	}

	if !config.IsSet(configApidInstanceID) {
		return pluginData, fmt.Errorf("Missing required config value: %s", configApidInstanceID)
	}
	apidInstanceID = config.GetString(configApidInstanceID)

	if !config.IsSet(configApidClusterID) {
		return pluginData, fmt.Errorf("Missing required config value: %s", configApidClusterID)
	}
	apidClusterID = config.GetString(configApidClusterID)

	config.SetDefault(configBundleDirKey, "bundles")
	config.SetDefault(configDebounceDuration, time.Second)
	config.SetDefault(configBundleCleanupDelay, time.Minute)
	config.SetDefault(configBundleDownloadTimeout, 5*time.Minute)

	debounceDuration = config.GetDuration(configDebounceDuration)
	if debounceDuration < time.Millisecond {
		return pluginData, fmt.Errorf("%s must be a positive duration", configDebounceDuration)
	}

	bundleCleanupDelay = config.GetDuration(configBundleCleanupDelay)
	if bundleCleanupDelay < time.Millisecond {
		return pluginData, fmt.Errorf("%s must be a positive duration", configBundleCleanupDelay)
	}

	bundleDownloadTimeout = config.GetDuration(configBundleDownloadTimeout)
	if bundleDownloadTimeout < time.Millisecond {
		return pluginData, fmt.Errorf("%s must be a positive duration", configBundleDownloadTimeout)
	}

	data = services.Data()

	relativeBundlePath := config.GetString(configBundleDirKey)
	storagePath := config.GetString("local_storage_path")
	bundlePath = path.Join(storagePath, relativeBundlePath)
	if err := os.MkdirAll(bundlePath, 0700); err != nil {
		return pluginData, fmt.Errorf("Failed bundle directory creation: %v", err)
	}
	log.Infof("Bundle directory path is %s", bundlePath)

	go distributeEvents()

	initListener(services)

	log.Debug("end init")

	return pluginData, nil
}
