/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package proxy

import (
	"bytes"
	"io"
	"testing"

	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
)

func Test_IsResponsePartialQuery(t *testing.T) {
	var query partialQuery
	res := query.IsResponse()
	assert.Equalf(t, res, false, "required to be false")

	res1 := query.GetOpCode()
	assert.NotNilf(t, res1, "not nil")

	res2 := query.Clone()
	assert.NotNilf(t, res2, "not nil")
}

func Test_IsResponsePartialExecute(t *testing.T) {
	var query partialExecute
	query.IsResponse()
}

func Test_EncodepartialQueryCodec(t *testing.T) {
	var v partialQueryCodec

	var writer io.Writer

	// Initialize it with a new bytes.Buffer
	buffer := new(bytes.Buffer)
	writer = buffer
	data := []byte("Hello, world!")
	_, err := writer.Write(data)
	assert.NoErrorf(t, err, "function should return no error")

	defer func() {
		if r := recover(); r != nil {
			t.Log("Recovered from panic:", r)
		}
	}()
	v.Encode(nil, writer, primitive.ProtocolVersion3)
}

func Test_EncodepartialQueryCodecEncodedLength(t *testing.T) {
	var v partialQueryCodec

	var writer io.Writer

	// Initialize it with a new bytes.Buffer
	buffer := new(bytes.Buffer)
	writer = buffer
	data := []byte("Hello, world!")
	_, err := writer.Write(data)
	assert.NoErrorf(t, err, "function should return no error")

	defer func() {
		if r := recover(); r != nil {
			t.Log("Recovered from panic:", r)
		}
	}()
	_, err = v.EncodedLength(nil, primitive.ProtocolVersion3)
	assert.Errorf(t, err, "function should return error")
}
func TestPartialExecute(t *testing.T) {
	pe := partialExecute{}
	res := pe.GetOpCode()
	assert.NotNilf(t, res, "function should return not nil")
}

func TestPartialExecuteClone(t *testing.T) {
	pe := partialExecute{
		queryId: []byte("testqueryid"),
	}
	res := pe.Clone()
	assert.NotNilf(t, res, "should not be nil")
}

func TestGetPositionalValues(t *testing.T) {
	data := []byte("test string")

	// byte slice to bytes.Reader, which implements the io.Reader interface
	reader := bytes.NewReader(data)
	_, err := getPositionalValues(reader)
	assert.Errorf(t, err, "error expected")
}

func TestPartialExecuteCodecDecode(t *testing.T) {
	pa := partialExecuteCodec{}
	data := []byte("test string")
	reader := bytes.NewReader(data)
	pa.Decode(reader, primitive.ProtocolVersion4)
}

func TestPartialBatchClone(t *testing.T) {

	pa := partialBatch{}
	res := pa.Clone()
	assert.NotNilf(t, res, "should not be nil")
}

func TestPartialQueryCodecDecode(t *testing.T) {
	pa := partialQueryCodec{}
	data := []byte("test string")
	reader := bytes.NewReader(data)
	pa.Decode(reader, primitive.ProtocolVersion3)
}

func Test_partialExecuteString(t *testing.T) {
	a := partialExecute{
		queryId: []byte("test"),
	}
	res := a.String()
	assert.NotNilf(t, res, "should not be nil")
}

func Test_partialExecuteCodecEncode(t *testing.T) {
	a := partialExecuteCodec{}
	var writer io.Writer

	// Initialize it with a new bytes.Buffer
	buffer := new(bytes.Buffer)
	writer = buffer
	data := []byte("Hello, world!")
	_, err := writer.Write(data)
	assert.NoErrorf(t, err, "function should return no error")

	defer func() {
		if r := recover(); r != nil {
			t.Log("Recovered from panic:", r)
		}
	}()
	a.Encode(nil, writer, primitive.ProtocolVersion3)
}

func Test_partialExecuteCodecDecode(t *testing.T) {
	a := partialExecuteCodec{}
	var writer io.Writer

	// Initialize it with a new bytes.Buffer
	buffer := new(bytes.Buffer)
	writer = buffer
	data := []byte("Hello, world!")
	_, err := writer.Write(data)
	assert.NoErrorf(t, err, "function should return no error")

	defer func() {
		if r := recover(); r != nil {
			t.Log("Recovered from panic:", r)
		}
	}()
	a.EncodedLength(nil, primitive.ProtocolVersion3)
}

func Test_partialBatchIsResponse(t *testing.T) {
	p := partialBatch{}
	res := p.IsResponse()
	assert.Equalf(t, false, res, "should return falses")
}

func Test_partialBatchGetOpCode(t *testing.T) {
	p := partialBatch{}
	res := p.GetOpCode()
	assert.NotNilf(t, res, "should not return nil")
}

func Test_partialBatchCodecEncode(t *testing.T) {
	p := partialBatchCodec{}
	var writer io.Writer

	// Initialize it with a new bytes.Buffer
	buffer := new(bytes.Buffer)
	writer = buffer
	data := []byte("Hello, world!")
	_, err := writer.Write(data)
	assert.NoErrorf(t, err, "function should return no error")

	defer func() {
		if r := recover(); r != nil {
			t.Log("Recovered from panic:", r)
		}
	}()
	p.Encode(nil, writer, primitive.ProtocolVersion3)
}

func Test_partialBatchCodecEncodedLength(t *testing.T) {
	p := partialBatchCodec{}
	var writer io.Writer

	// Initialize it with a new bytes.Buffer
	buffer := new(bytes.Buffer)
	writer = buffer
	data := []byte("Hello, world!")
	_, err := writer.Write(data)
	assert.NoErrorf(t, err, "function should return no error")

	defer func() {
		if r := recover(); r != nil {
			t.Log("Recovered from panic:", r)
		}
	}()
	p.EncodedLength(nil, primitive.ProtocolVersion3)
}

func Test_partialBatchCodecGetOpCode(t *testing.T) {
	p := partialBatchCodec{}
	res := p.GetOpCode()
	assert.NotNilf(t, res, "should not return nil")
}

func TestDecode_InvalidBatchType(t *testing.T) {
	inputData := []byte{
		0xFF,       // Invalid batch type
		0x00, 0x01, // Number of queries
		0x00,       // First query type
		0x00, 0x05, // Length of the query string
		'h', 'e', 'l', 'l', 'o',
	}

	source := bytes.NewReader(inputData)
	codec := partialBatchCodec{}

	// Call Decode
	msg, err := codec.Decode(source, primitive.ProtocolVersion4)

	// Assertions
	assert.Nil(t, msg)
	assert.ErrorContains(t, err, "invalid BATCH type: BatchType ?")
}

func TestDecode_ReadError(t *testing.T) {
	// Simulating an incomplete read or an error
	source := &bytes.Buffer{}
	codec := partialBatchCodec{}

	// Call Decode with insufficient data
	msg, err := codec.Decode(source, primitive.ProtocolVersion4)

	// Assertions
	assert.Nil(t, msg)
	assert.ErrorContains(t, err, "cannot read BATCH type: cannot read [byte]: EOF")
}

func TestDecode_UnsupportedBatchChildType(t *testing.T) {
	inputData := []byte{
		0x00,       // Valid batch type
		0x00, 0x01, // Number of queries
		0xFF, // Unsupported child type
	}

	source := bytes.NewReader(inputData)
	codec := partialBatchCodec{}

	// Call Decode
	msg, err := codec.Decode(source, primitive.ProtocolVersion4)

	// Assertions
	assert.Nil(t, msg)
	assert.EqualError(t, err, "unsupported BATCH child type for child #0: 255")
}
