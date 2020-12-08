package cache

import (
	"strconv"
	"time"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cerr "github.com/pip-services3-go/pip-services3-commons-go/errors"
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	cauth "github.com/pip-services3-go/pip-services3-components-go/auth"
	ccon "github.com/pip-services3-go/pip-services3-components-go/connect"

	redis "github.com/gomodule/redigo/redis"
)

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
    - db_num:                database number in Redis  (default 0)
    - max_size:            maximum number of values stored in this cache (default: 1000)

References:

- *:discovery:*:*:1.0        (optional) IDiscovery services to resolve connection
- *:credential-store:*:*:1.0 (optional) Credential stores to resolve credential

Example:

    cache = NewRedisCache();
    cache.Configure(cconf.NewConfigParamsFromTuples(
      "host", "localhost",
      "port", 6379,
    ));

    err = cache.Open("123")
      ...

    ret, err := cache.Store("123", "key1", []byte("ABC"))
    if err != nil {
    	...
    }

    res, err := cache.Retrive("123", "key1")
    value, _ := res.([]byte)
    fmt.Println(string(value))     // Result: "ABC"
*/
type RedisCache struct {
	connectionResolver *ccon.ConnectionResolver
	credentialResolver *cauth.CredentialResolver

	timeout int
	//retries int
	dbNum int

	client redis.Conn
}

// NewRedisCache method are creates a new instance of this cache.
func NewRedisCache() *RedisCache {
	c := RedisCache{}
	c.connectionResolver = ccon.NewEmptyConnectionResolver()
	c.credentialResolver = cauth.NewEmptyCredentialResolver()
	c.timeout = 30000
	//c.retries = 3
	c.dbNum = 0
	return &c
}

// Configure method are configures component by passing configuration parameters.
//   - config    configuration parameters to be set.
func (c *RedisCache) Configure(config *cconf.ConfigParams) {
	c.connectionResolver.Configure(config)
	c.credentialResolver.Configure(config)

	c.timeout = config.GetAsIntegerWithDefault("options.timeout", c.timeout)
	//c.retries = config.GetAsIntegerWithDefault("options.retries", c.retries)
	c.dbNum = config.GetAsIntegerWithDefault("options.db_num", c.dbNum)
	if c.dbNum > 15 || c.dbNum < 0 {
		c.dbNum = 0
	}
}

// Sets references to dependent components.
//   - references 	references to locate the component dependencies.
func (c *RedisCache) SetReferences(references cref.IReferences) {
	c.connectionResolver.SetReferences(references)
	c.credentialResolver.SetReferences(references)
}

// Checks if the component is opened.
// Returns true if the component has been opened and false otherwise.
func (c *RedisCache) IsOpen() bool {
	return c.client != nil
}

// Open method are opens the component.
// Parameters:
//  - correlationId 	(optional) transaction id to trace execution through call chain.
// Returns: error or nil no errors occured.
func (c *RedisCache) Open(correlationId string) error {
	var connection *ccon.ConnectionParams
	var credential *cauth.CredentialParams

	connection, err := c.connectionResolver.Resolve(correlationId)

	if err == nil && connection == nil {
		err = cerr.NewConfigError(correlationId, "NO_CONNECTION", "Connection is not configured")
		return err
	}

	credential, err = c.credentialResolver.Lookup(correlationId)
	if err != nil {
		return err
	}

	var url, host, port, password string
	var dialOpts []redis.DialOption = make([]redis.DialOption, 0)

	dialOpts = append(dialOpts, redis.DialConnectTimeout(time.Duration(c.timeout)*time.Millisecond))
	dialOpts = append(dialOpts, redis.DialDatabase(c.dbNum))

	if credential != nil {
		password = credential.Password()
		dialOpts = append(dialOpts, redis.DialPassword(password))
	}

	if connection.Uri() != "" {
		url = connection.Uri()
		c.client, err = redis.DialURL(url, dialOpts...)
	} else {
		host = connection.Host()
		if host == "" {
			host = "localhost"
		}
		port = strconv.FormatInt(int64(connection.Port()), 10)
		if port == "0" {
			port = "6379"
		}
		url = host + ":" + port
		c.client, err = redis.Dial("tcp", url, dialOpts...)
	}
	return err
}

// Close method are closes component and frees used resources.
// Parameters:
//   - correlationId 	(optional) transaction id to trace execution through call chain.
// Retruns: error or nil no errors occured.
func (c *RedisCache) Close(correlationId string) error {
	if c.client != nil {
		err := c.client.Close()
		c.client = nil
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *RedisCache) checkOpened(correlationId string) (state bool, err error) {
	if !c.IsOpen() {
		err = cerr.NewInvalidStateError(correlationId, "NOT_OPENED", "Connection is not opened")
		return false, err
	}

	return true, nil
}

// Retrieve method are retrieves cached value from the cache using its key.
// If value is missing in the cache or expired it returns nil.
// Parameters:
//   - correlationId     (optional) transaction id to trace execution through call chain.
//   - key               a unique value key.
//  Retruns: cached value or error.
func (c *RedisCache) Retrieve(correlationId string, key string) (value interface{}, err error) {
	state, err := c.checkOpened(correlationId)
	if !state {
		return nil, err
	}
	return c.client.Do("GET", key)
}

// Store method are stores value in the cache with expiration time.
// Parameters:
//   - correlationId     (optional) transaction id to trace execution through call chain.
//   - key               a unique value key.
//   - value             a value to store.
//   - timeout           expiration timeout in milliseconds.
// Retruns error or nil for success
func (c *RedisCache) Store(correlationId string, key string, value interface{}, timeout int64) (result interface{}, err error) {
	state, err := c.checkOpened(correlationId)
	if !state {
		return nil, err
	}

	return c.client.Do("SET", key, value, "PX", timeout)
}

// Removes a value from the cache by its key.
// Parameters:
//   - correlationId     (optional) transaction id to trace execution through call chain.
//   - key               a unique value key.
// Returns: error or nil for success
func (c *RedisCache) Remove(correlationId string, key string) error {
	state, err := c.checkOpened(correlationId)
	if !state {
		return err
	}
	_, err = c.client.Do("DEL", key)
	return err
}
