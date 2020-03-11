package cache

import (
	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	// cerr "github.com/pip-services3-go/pip-services3-commons-go/errors"
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	cauth "github.com/pip-services3-go/pip-services3-components-go/auth"
	ccon "github.com/pip-services3-go/pip-services3-components-go/connect"
)

// import { ConfigParams } from "pip-services3-commons-node";
// import { IConfigurable } from "pip-services3-commons-node";
// import { IReferences } from "pip-services3-commons-node";
// import { IReferenceable } from "pip-services3-commons-node";
// import { IOpenable } from "pip-services3-commons-node";
// import { InvalidStateException } from "pip-services3-commons-node";
// import { ConfigException } from "pip-services3-commons-node";
// import { ConnectionParams } from "pip-services3-components-node";
// import { ConnectionResolver } from "pip-services3-components-node";
// import { CredentialParams } from "pip-services3-components-node";
// import { CredentialResolver } from "pip-services3-components-node";
// import { ICache } from "pip-services3-components-node";

/*
Distributed cache that stores values in Redis in-memory database.

Configuration parameters:

- connection(s):
  - discovery_key:         (optional) a key to retrieve the connection from IDiscovery
  - host:                  host name or IP address
  - port:                  port number
  - uri:                   resource URI or connection string with all parameters in it
- credential(s):
  - store_key:             key to retrieve parameters from credential store
  - username:              user name (currently is not used)
  - password:              user password
- options:
  - retries:               number of retries (default: 3)
  - timeout:               default caching timeout in milliseconds (default: 1 minute)
  - max_size:              maximum number of values stored in this cache (default: 1000)

References:

- *:discovery:*:*:1.0        (optional) IDiscovery services to resolve connection
- *:credential-store:*:*:1.0 (optional) Credential stores to resolve credential

Example:

    let cache = new RedisCache();
    cache.configure(ConfigParams.fromTuples(
      "host", "localhost",
      "port", 6379
    ));

    cache.open("123", (err) => {
      ...
    });

    cache.store("123", "key1", "ABC", (err) => {
         cache.store("123", "key1", (err, value) => {
             // Result: "ABC"
         });
    });
*/

type RedisCache struct {
	connectionResolver *ccon.ConnectionResolver
	credentialResolver *cauth.CredentialResolver

	timeout int
	retries int

	client interface{}
}

// NewRedisCache method are creates a new instance of this cache.
func NewRedisCache() *RedisCache {
	c := RedisCache{}
	c.connectionResolver = ccon.NewEmptyConnectionResolver()
	c.credentialResolver = cauth.NewEmptyCredentialResolver()
	c.timeout = 30000
	c.retries = 3
	return &c
}

// Configure method are configures component by passing configuration parameters.
//  - config    configuration parameters to be set.
func (c *RedisCache) Configure(config *cconf.ConfigParams) {
	c.connectionResolver.Configure(config)
	c.credentialResolver.Configure(config)

	c.timeout = config.GetAsIntegerWithDefault("options.timeout", c.timeout)
	c.retries = config.GetAsIntegerWithDefault("options.retries", c.retries)
}

// Sets references to dependent components.
// 	- references 	references to locate the component dependencies.
func (c *RedisCache) SetReferences(references cref.IReferences) {
	c.connectionResolver.SetReferences(references)
	c.credentialResolver.SetReferences(references)
}

// Checks if the component is opened.
// Returns true if the component has been opened and false otherwise.
func (c *RedisCache) IsOpen() bool {
	return c.client != nil
}

//     /*
// 	Opens the component.
// 	 *
// 	- correlationId 	(optional) transaction id to trace execution through call chain.
//     - callback 			callback function that receives error or null no errors occured.
//      */
//     func (c* RedisCache) open(correlationId: string, callback: (err: any) => void) {
//         let connection: ConnectionParams;
//         let credential: CredentialParams;

//         async.series([
//             (callback) => {
//                 c.connectionResolver.resolve(correlationId, (err, result) => {
//                     connection = result;
//                     if (err == null && connection == null)
//                         err = new ConfigException(correlationId, "NO_CONNECTION", "Connection is not configured");
//                     callback(err);
//                 });
//             },
//             (callback) => {
//                 c.credentialResolver.lookup(correlationId, (err, result) => {
//                     credential = result;
//                     callback(err);
//                 });
//             },
//             (callback) => {
//                 let options: any = {
//                     // connecttimeout: c.timeout,
//                     // max_attempts: c.retries,
//                     retry_strategy: (options) => { return c.retryStrategy(options); }
//                 };

//                 if (connection.getUri() != null) {
//                     options.url = connection.getUri();
//                 } else {
//                     options.host = connection.getHost() || "localhost";
//                     options.port = connection.getPort() || 6379;
//                 }

//                 if (credential != null) {
//                     options.password = credential.getPassword();
//                 }

//                 let redis = require("redis");
//                 c.client = redis.createClient(options);

//                 if (callback) callback(null);
//             }
//         ], callback);
//     }

//     /*
// 	Closes component and frees used resources.
// 	 *
// 	- correlationId 	(optional) transaction id to trace execution through call chain.
//     - callback 			callback function that receives error or null no errors occured.
//      */
//     func (c* RedisCache) close(correlationId: string, callback: (err: any) => void) {
//         if (c.client != null) {
//             c.client.quit(((err) => {
//                 c.client = null;
//                 if (callback) callback(err);
//             }));
//         } else {
//             if (callback) callback(null);
//         }
//     }

//     private checkOpened(correlationId: string, callback: any): boolean {
//         if (!c.isOpen()) {
//             let err = new InvalidStateException(correlationId, "NOT_OPENED", "Connection is not opened");
//             callback(err, null);
//             return false;
//         }

//         return true;
//     }

//     private retryStrategy(options: any): any {
//         if (options.error && options.error.code === "ECONNREFUSED") {
//             // End reconnecting on a specific error and flush all commands with
//             // a individual error
//             return new Error("The server refused the connection");
//         }
//         if (options.total_retry_time > c.timeout) {
//             // End reconnecting after a specific timeout and flush all commands
//             // with a individual error
//             return new Error("Retry time exhausted");
//         }
//         if (options.attempt > c.retries) {
//             // End reconnecting with built in error
//             return undefined;
//         }
//         // reconnect after
//         return Math.min(options.attempt100, 3000);
//     }

//     /*
//     Retrieves cached value from the cache using its key.
//     If value is missing in the cache or expired it returns null.
//      *
//     - correlationId     (optional) transaction id to trace execution through call chain.
//     - key               a unique value key.
//     - callback          callback function that receives cached value or error.
//      */
//     func (c* RedisCache) retrieve(correlationId: string, key: string,
//         callback: (err: any, value: any) => void) {
//         if (!c.checkOpened(correlationId, callback)) return;

//         c.client.get(key, callback);
//     }

//     /*
//     Stores value in the cache with expiration time.
//      *
//     - correlationId     (optional) transaction id to trace execution through call chain.
//     - key               a unique value key.
//     - value             a value to store.
//     - timeout           expiration timeout in milliseconds.
//     - callback          (optional) callback function that receives an error or null for success
//      */
//     func (c* RedisCache) store(correlationId: string, key: string, value: any, timeout: number,
//         callback: (err: any) => void) {
//         if (!c.checkOpened(correlationId, callback)) return;

//         c.client.set(key, value, "PX", timeout, callback);
//     }

//     /*
//     Removes a value from the cache by its key.
//      *
//     - correlationId     (optional) transaction id to trace execution through call chain.
//     - key               a unique value key.
//     - callback          (optional) callback function that receives an error or null for success
//      */
//     func (c* RedisCache) remove(correlationId: string, key: string,
//         callback: (err: any) => void) {
//         if (!c.checkOpened(correlationId, callback)) return;

//         c.client.del(key, callback);
//     }

// }
