# apidGatewayDeploy

This core plugin for [apid](http://github.com/30x/apid) responds to 
[apidApigeeSync](https://github.com/30x/apidApigeeSync) events and publishes an API that allows clients to 
deploy and update a locally configured gateway.

## Functional description

This plugin simply tracks counters based on called URIs:
 
* `GET /deployments/` - retrieve current deployment
* `POST /deployments/` - update deployments

See [apidGatewayDeploy-api.yaml]() for full spec.

## Configuration

#### gatewaydeploy_debounce_duration
Window of time during which deployment changes are gathered before sending to client.
Default: "bundles"

#### gatewaydeploy_bundle_cleanup_delay
Duration between deleting a deployment and deleting it's bundles on disk. 
Default: "1s"

#### gatewaydeploy_bundle_download_timeout
Duration before bundle download marks deployment as failed (will continue retries regardless). 
Default: "1m"

#### gatewaydeploy_bundle_dir
Relative location from local_storage_path in which to store local bundle files.
Default: "5m"

(durations note, see: https://golang.org/pkg/time/#ParseDuration)

## Building and running standalone

First, install prerequisites:
 
    glide install

To run an apid test instance with just this plugin installed, change to the `cmd/apidGatewayDeploy` folder. 
From here, you may create an executable with: 

    go build 
  
Alternatively, you may run without creating an executable with:

    go run main.go 
    
Command line options:

* -deployments <file path>

If you use the `-deployments` option, the server will start using a clean database that contains only the
 deployments contained in the file specified. 
 
The file should be the JSON for an array of deployments. JSON format is:

        [
          {
            "id": "",
            "scopeId": "",
            "created": "",
            "createdBy": "",
            "updated": "",
            "updatedBy": "",
            "configuration": {
              "key": "value"
            },
            "bundleConfiguration": {
              "key": "value"
            },
            "displayName": "",
            "uri": ""
          }
        ]
 
Notes:
* id must be unique
* uri should point to a bundle file
 
Once the process is running, you should be able to manually give the plugin's API a whirl...

    curl -i localhost:9000/deployments/
    curl -i -X POST localhost:9000/deployments -d '{ ... }'

The following may be interesting env vars for configuration:

* APID_API_PORT

## Running tests

To run the tests, just run:

    go test
    
To generate coverage, you may run:

    ./cover.sh

Then open `cover.html` with your browser to see details.
