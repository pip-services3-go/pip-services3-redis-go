package lock

import (

	// cerr "github.com/pip-services3-go/pip-services3-commons-go/errors"
	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	cauth "github.com/pip-services3-go/pip-services3-components-go/auth"
	ccon "github.com/pip-services3-go/pip-services3-components-go/connect"
	clock "github.com/pip-services3-go/pip-services3-components-go/lock"
)

// import { ConfigParams } from "pip-services3-commons-node";
// import { IConfigurable } from "pip-services3-commons-node";
// import { IReferences } from "pip-services3-commons-node";
// import { IReferenceable } from "pip-services3-commons-node";
// import { IOpenable } from "pip-services3-commons-node";
// import { IdGenerator } from "pip-services3-commons-node";
// import { InvalidStateException } from "pip-services3-commons-node";
// import { ConfigException } from "pip-services3-commons-node";
// import { ConnectionParams } from "pip-services3-components-node";
// import { ConnectionResolver } from "pip-services3-components-node";
// import { CredentialParams } from "pip-services3-components-node";
// import { CredentialResolver } from "pip-services3-components-node";
// import { Lock } from "pip-services3-components-node";

/*
RedisLock are distributed lock that is implemented based on Redis in-memory database.

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
  - retrytimeout:         timeout in milliseconds to retry lock acquisition. (Default: 100)
  - retries:               number of retries (default: 3)

References:

- *:discovery:*:*:1.0        (optional) IDiscovery services to resolve connection
- *:credential-store:*:*:1.0 (optional) Credential stores to resolve credential

Example:

    let lock = new RedisRedis();
    lock.configure(ConfigParams.fromTuples(
      "host", "localhost",
      "port", 6379
    ));

    lock.open("123", (err) => {
      ...
    });

    lock.acquire("123", "key1", (err) => {
         if (err == null) {
             try {
               // Processing...
             } finally {
                lock.releaseLock("123", "key1", (err) => {
                    // Continue...
                });
             }
         }
    });
*/
type RedisLock struct {
	*clock.Lock
	connectionResolver *ccon.ConnectionResolver
	credentialResolver *cauth.CredentialResolver

	lock    string
	timeout int
	retries int

	client interface{}
}

// NewRedisLock method are creates a new instance of this lock.
func NewRedisLock() *RedisLock {
	c := RedisLock{}
	c.connectionResolver = ccon.NewEmptyConnectionResolver()
	c.credentialResolver = cauth.NewEmptyCredentialResolver()

	c.lock = cdata.IdGenerator.NextLong()
	c.timeout = 30000
	c.retries = 3

	c.client = nil
	return &c
}

//   Configure method are configures component by passing configuration parameters.
//   - config    configuration parameters to be set.
func (c *RedisLock) Configure(config *cconf.ConfigParams) {
	c.connectionResolver.Configure(config)
	c.credentialResolver.Configure(config)

	c.timeout = config.GetAsIntegerWithDefault("options.timeout", c.timeout)
	c.retries = config.GetAsIntegerWithDefault("options.retries", c.retries)
}

// SetReferences method are sets references to dependent components.
// - references 	references to locate the component dependencies.
func (c *RedisLock) SetReferences(references cref.IReferences) {
	c.connectionResolver.SetReferences(references)
	c.credentialResolver.SetReferences(references)
}

// IsOpen method are checks if the component is opened.
// Returns true if the component has been opened and false otherwise.
func (c *RedisLock) IsOpen() bool {
	return c.client != nil
}

//     /**
// 	Opens the component.
// 	 *
// 	- correlationId 	(optional) transaction id to trace execution through call chain.
//     - callback 			callback function that receives error or null no errors occured.
//      */
//     func (c*RedisLock) open(correlationId: string, callback: (err: any) => void) {
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

//     /**
// 	Closes component and frees used resources.
// 	 *
// 	- correlationId 	(optional) transaction id to trace execution through call chain.
//     - callback 			callback function that receives error or null no errors occured.
//      */
//     func (c*RedisLock) close(correlationId: string, callback: (err: any) => void) {
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

//     /**
//     Makes a single attempt to acquire a lock by its key.
//     It returns immediately a positive or negative result.
//      *
//     - correlationId     (optional) transaction id to trace execution through call chain.
//     - key               a unique lock key to acquire.
//     - ttl               a lock timeout (time to live) in milliseconds.
//     - callback          callback function that receives a lock result or error.
//      */
//     func (c*RedisLock) tryAcquireLock(correlationId: string, key: string, ttl: number,
//         callback: (err: any, result: boolean) => void) {
//         if (!c.checkOpened(correlationId, callback)) return;

//         c.client.set(key, c.lock, "NX", "PX", ttl, (err, result) => {
//             callback(err, result == "OK");
//         });
//     }

//     /**
//     Releases prevously acquired lock by its key.
//      *
//     - correlationId     (optional) transaction id to trace execution through call chain.
//     - key               a unique lock key to release.
//     - callback          callback function that receives error or null for success.
//      */
//     func (c*RedisLock) releaseLock(correlationId: string, key: string,
//         callback?: (err: any) => void) {
//         if (!c.checkOpened(correlationId, callback)) return;

//         // Start transaction on key
//         c.client.watch(key, (err) => {
//             if (err) {
//                 if (callback) callback(err);
//                 return;
//             }

//             // Read and check if lock is the same
//             c.client.get(key, (err, result) => {
//                 if (err) {
//                     if (callback) callback(err);
//                     return;
//                 }

//                 // Remove the lock if it matches
//                 if (result == c.lock) {
//                     c.client.multi()
//                         .del(key)
//                         .exec(callback);
//                 }
//                 // Cancel transaction if it doesn"t match
//                 else {
//                     c.client.unwatch(callback);
//                 }
//             })
//         });
//     }
// }
