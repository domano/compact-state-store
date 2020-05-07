package store

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

func TestStore_WriteAndRead(t *testing.T) {
	// given
	path, _ := ioutil.TempDir("", "")
	testStore, err := OpenStore(path)
	assert.NoError(t, err)
	//defer testStore.Close()

	// write
	k1, v1 := "one", "wubdakdba"
	k2, v2 := "two", "asdnladn"
	k3, v3 := "three", "asdiohaliahdald"
	k4, v4 := "four", "haoihaoidhda"
	assert.NoError(t, testStore.Write([]byte(k1),[]byte(v1)))
	assert.NoError(t, testStore.Write([]byte(k2),[]byte(v2)))
	assert.NoError(t, testStore.Write([]byte(k3),[]byte(v3)))
	assert.NoError(t, testStore.Write([]byte(k4),[]byte(v4)))
	//testStore.Close()

	testReader, err := testStore.Reader(0)
	assert.NoError(t, err)
	// read
	rk1, rv1, err := testReader.Read()
	assert.NoError(t, err)
	assert.Equal(t, k1, rk1)
	assert.EqualValues(t, v1, rv1)
	rk2, rv2, err := testReader.Read()
	assert.NoError(t, err)
	assert.Equal(t, k2, rk2)
	assert.EqualValues(t, v2, rv2)
	rk3, rv3, err := testReader.Read()
	assert.NoError(t, err)
	assert.Equal(t, k3, rk3)
	assert.EqualValues(t, v3, rv3)
	rk4, rv4, err := testReader.Read()
	assert.Equal(t, k4, rk4)
	assert.EqualValues(t, v4, rv4)
	assert.NoError(t, err)

}
