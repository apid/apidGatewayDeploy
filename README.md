# apidVerifyAPIKey

This core plugin for [apid](http://github.com/30x/apid) responds to 
[apidApigeeSync](https://github.com/30x/apidApigeeSync) events and publishes an API that allows clients to 
deploy and update a locally configured gateway.

## Functional description

This plugin simply tracks counters based on called URIs:
 
* `GET /deployments/current` - retrieve current deployment
* `POST /deployments/{id}` - update specified deployment 

## Building and running standalone

First, install prerequisites:
 
    glide install

To run an apid test instance with just this plugin installed, change to the `cmd/apidGatewayDeploy` folder. 
From here, you may create an executable with: 

    go build 
  
Alternatively, you may run without creating an executable with:

    go run main.go 
    
Command line options:

* -manifest <file path>

If you use the `-manifest` option, the server will start using a clean database that contains only the
 deployment manifest specified. 
 
Once the process is running, you should be able to manually give the plugin's API a whirl...

    curl -i localhost:9000/deployments/current 
    curl -i -X POST localhost:9000/deployments/entityId -d '{ "status": "SUCCESS" }' 

The following have been exposed as configurable env vars:

* APID_API_PORT

## Running tests

To run the tests, just run:

    go test
    
To generate coverage, you may run:

    ./cover.sh

Then open `cover.html` with your browser to see details.
