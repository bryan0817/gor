/*
$ export GOPATH=/Users/liubryan/workApp/workspace/gor-redis/
$ export GOOS=linux
$ export GOARCH=amd64
$ cd /Users/liubryan/workApp/workspace/gor-redis/src/github.com/bryan0817/gor
$ go build -o ./gor-$GOOS-$GOARCH
Run it:
./gor-linux-amd64 --input-file "request.1.gor" --output-redis "192.168.101.170:6379"
*/
package main

import (
	"io"
	"os"
	"fmt"
	"bytes"
	"strings"
	"encoding/base64"
	"gopkg.in/redis.v3"
	"time"
	"strconv"
)

// RedisOutput output plugin
type RedisOutput struct {
	url string //"192.168.101.170:6379"
	client *redis.Client
}

// NewRedisOutput constructor
func NewRedisOutput(url string) io.Writer {
	o := new(RedisOutput)
	o.url = url
	o.init(url)

	return o
}

func (o *RedisOutput) init(url string) {

	o.client = redis.NewClient(&redis.Options{
		Addr: url,
		DB:   0,  // use default DB
	})

}

func (o *RedisOutput) Write(data []byte) (n int, err error) {
	if !isOriginPayload(data) {
		return len(data), nil
	}
	ssp, _time, body := process(data)
	if (len(ssp) > 0) {
		sEnc := base64.StdEncoding.EncodeToString(body)
		ts, _ := strconv.ParseFloat(_time, 64)
		// Epoch time - UnixNano() convert to milliseconds
		ts = ts / float64(time.Millisecond)

		if (len(ssp) == 0) {
			Debug("Return due to no ssp (Host/PoST method) found")
			return len(data), nil
		}
		o.client.ZAdd("qa-" + ssp, redis.Z{ts, sEnc}) // redis sorted set
		//o.client.SAdd(ssp, sEnc) // redis set
	}
	return len(data), nil
}

func process(buf []byte) (ssp string, timeStamp string, body []byte) {
	// First byte indicate payload type, possible values:
	//  1 - Request
	//  2 - Response
	//  3 - ReplayedResponse
	payloadType := buf[0]
	reqBodyIndex := -1
	var sspName, timestamp string

	switch payloadType {
	case '1': // Request
		headerSize := bytes.IndexByte(buf, '\n') + 1
		header := buf[:headerSize-1]
		fmt.Fprintln(os.Stderr, "Parsing request with header:", string(header))
		// Header contains space separated values of: request type, request id, and request start time (or round-trip time for responses)
		meta := bytes.Split(header, []byte(" "))
		// For each request you should receive 3 payloads (request, response, replayed response) with same request id
		timestamp = strings.TrimSpace(string(meta[2]))
		payload := buf[headerSize:]
		method := strings.TrimSpace(string(payload[0:4]))
		Debug("[OUTPUTTER] HTTP Method: ", method)
		if("POST" != method) {
			return "", "",  nil
		}
		bodyLines := bytes.Split(payload, []byte("\n"))
		r := strings.NewReplacer(
			"HOST:", "",
			"Host:", "",
			"host:", "",
			"-ssp.dsp.vpadn.com", "",
			"-adx.vpadn.com", "")
		for i := 0; i <= len(bodyLines); i++ {
			if bytes.Equal(bodyLines[i],[]byte("\r")) || len(bodyLines[i]) == 0 {
				Debug("[OUTPUTTER] Check header: ", i , ": ", bodyLines[i])
				reqBodyIndex = bytes.Index(buf, bodyLines[i-1]) + len(bodyLines[i-1]) + 1 + len("\r") + len("\n")
				// next one would be actual request body
				// !!append the rest of line[i] to a buffer then its the body!!
				break
			} else {
				// headers => check HOST value
				if Settings.debug {
					Debug("[OUTPUTTER] Check header: ", i , ": ", bodyLines[i])
					Debug("[OUTPUTTER] Check header: ", bytes.Contains(bytes.ToLower(bodyLines[i]), []byte("host:")))
				}
				if bytes.Contains(bytes.ToLower(bodyLines[i]), []byte("host:")) {
					sspName = strings.TrimSpace(r.Replace(string(bodyLines[i])))
				}
			}
		}

	case '2': // Original response
		//only do request
		Debug("[OUTPUTTER] into case 2")
		return "", "", buf
	case '3': // Replayed response
		//only do request
		Debug("[OUTPUTTER] into case 3")
		return "", "", buf
	}
	result := buf[reqBodyIndex:]
	if Settings.debug {
		Debug("[OUTPUTTER] Received timestamp:", timestamp)
		Debug("[OUTPUTTER] Received 'HOST' header:", sspName)
		Debug("[OUTPUTTER] Received request payload:", result)
	}
	return sspName, timestamp, result
}
