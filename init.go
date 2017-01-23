package apiGatewayDeploy

import (
	"github.com/30x/apid"
	"os"
	"path"
	"time"
)

const (
	configBundleDirKey          = "gatewaydeploy_bundle_dir"
	configDebounceDuration      = "gatewaydeploy_debounce_duration"
	configBundleCleanupDelay    = "gatewaydeploy_bundle_cleanup_delay"
	configBundleDownloadTimeout = "gatewaydeploy_bundle_download_timeout"
)

var (
	services           apid.Services
	log                apid.LogService
	data               apid.DataService
	bundlePath         string
	debounceDuration   time.Duration
	bundleCleanupDelay time.Duration
)

func init() {
	apid.RegisterPlugin(initPlugin)
}

func initPlugin(s apid.Services) (apid.PluginData, error) {
	services = s
	log = services.Log().ForModule("apiGatewayDeploy")
	log.Debug("start init")

	config := services.Config()
	config.SetDefault(configBundleDirKey, "bundles")
	config.SetDefault(configDebounceDuration, time.Second)
	config.SetDefault(configBundleCleanupDelay, time.Minute)
	config.SetDefault(configBundleDownloadTimeout, 5 * time.Minute)

	debounceDuration = config.GetDuration(configDebounceDuration)
	if debounceDuration < time.Millisecond {
		log.Panicf("%s must be a positive duration", configDebounceDuration)
	}

	bundleCleanupDelay = config.GetDuration(configBundleCleanupDelay)
	if bundleCleanupDelay < time.Millisecond {
		log.Panicf("%s must be a positive duration", configBundleCleanupDelay)
	}

	bundleDownloadTimeout = config.GetDuration(configBundleDownloadTimeout)
	if bundleDownloadTimeout < time.Millisecond {
		log.Panicf("%s must be a positive duration", configBundleDownloadTimeout)
	}

	data = services.Data()

	relativeBundlePath := config.GetString(configBundleDirKey)
	storagePath := config.GetString("local_storage_path")
	bundlePath = path.Join(storagePath, relativeBundlePath)
	if err := os.MkdirAll(bundlePath, 0700); err != nil {
		log.Panicf("Failed bundle directory creation: %v", err)
	}
	log.Infof("Bundle directory path is %s", bundlePath)

	go distributeEvents()

	initListener(services)

	log.Debug("end init")

	return pluginData, nil
}
