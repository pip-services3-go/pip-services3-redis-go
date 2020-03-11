package test_fixture

import (
	"testing"
	"time"

	ccache "github.com/pip-services3-go/pip-services3-components-go/cache"
	"github.com/stretchr/testify/assert"
)

const (
	KEY1   string = "key1"
	KEY2   string = "key2"
	VALUE1 string = "value1"
	VALUE2 string = "value2"
)

type CacheFixture struct {
	cache ccache.ICache
}

func NewCacheFixture(cache ccache.ICache) *CacheFixture {
	c := CacheFixture{}
	c.cache = cache
	return &c
}

func (c *CacheFixture) TestStoreAndRetrieve(t *testing.T) {

	_, err := c.cache.Store("", KEY1, []byte(VALUE1), 5000)
	assert.Nil(t, err)

	_, err = c.cache.Store("", KEY2, []byte(VALUE2), 5000)
	assert.Nil(t, err)

	select {
	case <-time.After(500 * time.Millisecond):
	}

	res, err := c.cache.Retrieve("", KEY1)
	val, _ := res.([]byte)

	assert.Nil(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, VALUE1, string(val))

	res, err = c.cache.Retrieve("", KEY2)
	val, _ = res.([]byte)

	assert.Nil(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, VALUE2, string(val))

}

func (c *CacheFixture) TestRetrieveExpired(t *testing.T) {

	_, err := c.cache.Store("", KEY1, []byte(VALUE1), 1000)
	assert.Nil(t, err)

	select {
	case <-time.After(1500 * time.Millisecond):
	}

	val, err := c.cache.Retrieve("", KEY1)
	assert.Nil(t, err)
	assert.Nil(t, val)

}

func (c *CacheFixture) TestRemove(t *testing.T) {

	_, err := c.cache.Store("", KEY1, []byte(VALUE1), 1000)
	assert.Nil(t, err)

	err = c.cache.Remove("", KEY1)
	assert.Nil(t, err)

	val, err := c.cache.Retrieve("", KEY1)
	assert.Nil(t, err)
	assert.Nil(t, val)
}
