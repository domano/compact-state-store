package store

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

//MessageSet (Version: 1) => [offset message_size message]
//    offset => INT64
//    message_size => INT32
//    message => crc magic_byte attributes key value
//        crc => INT32
//        magic_byte => INT8
//        attributes => INT8
//            bit 0~2:
//                0: no compression
//                1: gzip
//                2: snappy
//                3: lz4
//            bit 3: timestampType
//                0: create time
//                1: log append time
//            bit 4~7: unused
//        timestamp =>INT64
//        key => BYTES
//        value => BYTES

type messageSet []byte

const (
	offsetSize, offsetStart, offsetEnd                = 8, 0, 8
	messageSizeSize, messageSizeStart, messageSizeEnd = 4, 8, 12
	crcSize, crcStart, crcEnd                         = 4, 12, 16
	magicByteSize, magicByteStart, magicByteEnd       = 1, 16, 17
	attributesSize, attributesStart, attributesEnd    = 1, 17, 18
	timestampSize, timestampStart, timestampEnd       = 8, 18, 26
	combinedSize                                      = offsetSize + messageSizeSize + crcSize + magicByteSize + attributesSize + timestampSize
)

type messageSetElement struct {
	offset       [8]byte
	message_size [4]byte
	message      message
}

type message struct {
	crc        [4]byte
	magic_byte byte
	attributes byte
	timestamp  [8]byte
	key        []byte
	value      []byte
}

const version byte = 1 // value for the magic byte, protocol version-ish

func newMessage(key, value []byte) message{
	timestamp := time.Now().Unix()
	timeBytes := [8]byte{}
	binary.BigEndian.PutUint64(timeBytes[:], uint64(timestamp))

	msg := message{
		magic_byte: version,
		attributes: 1 << 3,
		timestamp:  timeBytes,
		key:        key,
		value:      value,
	}
	var data = make([]byte, 10+4+len(key)+4+len(value))
	data[0] = msg.magic_byte
	data[1] = msg.attributes
	copy(data[2:10], msg.timestamp[:])
	binary.BigEndian.PutUint32(data[10:14], uint32(len(msg.key)))
	copy(data[14:14+len(msg.key)], msg.key)
	binary.BigEndian.PutUint32(data[14+len(msg.key):14+len(msg.key)+4], uint32(len(msg.value)))
	copy(data[14+len(msg.key)+4:], msg.value)

	crc := crc32.ChecksumIEEE(data)

	binary.BigEndian.PutUint32(msg.crc[:], crc)
	msg.magic_byte = version
	msg.attributes = 1 << 3
	return msg
}

func newMessageSet(msgs ...message) messageSet {
	var ms []byte
	for i := range msgs {
		msg := msgs[i]
		var e = make([]byte, combinedSize) // create a byte slice exactly as big as the given element requires it

		// Add all message fields sequentially
		copy(e[crcStart:crcEnd], msg.crc[:])
		e[magicByteStart] = msg.magic_byte
		e[attributesStart] = msg.attributes
		copy(e[timestampStart:timestampEnd], msg.timestamp[:])
		e = append(e, msg.key...)
		e = append(e, msg.value...)

		// and calculate the message size to set it
		binary.BigEndian.PutUint32(e[messageSizeStart:messageSizeEnd], uint32(len(e[crcStart:])))

		// Add element to the message set
		ms = append(ms, e...)
	}
	return ms
}
