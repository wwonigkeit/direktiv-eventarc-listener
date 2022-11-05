package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// CE store components of the cloud event
type CE struct {
	Id          string    `json:"id"`
	Source      string    `json:"source"`
	Specversion string    `json:"specversion"`
	Data        []byte    `json:"data"`
	Ctype       string    `json:"type"`
	Ctime       time.Time `json:"time"`
}

// logCloudEvent logs all information about cloud event
func logCloudEvent(ce *CE) error {
	data, err := json.Marshal(ce)
	if err != nil {
		return err
	}
	log.Printf("Cloud event received: \n%s", data)
	return nil
}

// readCloudEvent stores the event in a struct from headers
func readCloudEvent(r *http.Request) (*CE, error) {
	// initial string replacements
	ce := &CE{
		Id:          r.Header.Get("ce-id"),
		Source:      r.Header.Get("ce-source"),
		Specversion: r.Header.Get("ce-specversion"),
		Ctype:       r.Header.Get("ce-type"),
	}

	// parse time back to readable format of time.time
	t, err := time.Parse(time.RFC3339, r.Header.Get("ce-time"))
	if err != nil {
		return nil, err
	}
	ce.Ctime = t

	// read data
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	ce.Data = body

	return ce, nil
}

// parseCloudEvent turns data to follow spec
func parseCloudEvent(ce *CE) cloudevents.Event {

	event := cloudevents.NewEvent()
	event.SetID(ce.Id)
	event.SetSource(ce.Source)
	event.SetSpecVersion(ce.Specversion)
	event.SetType(ce.Ctype)
	event.SetTime(ce.Ctime)
	event.SetData(cloudevents.ApplicationJSON, ce.Data)

	return event
}

// sendCloudEvent back to Direktiv
func sendCloudEvent(event cloudevents.Event) ([]byte, error) {
	// unmarshal cloud event to relay back to direktiv
	data, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/namespaces/%s/broadcast", os.Getenv("DIREKTIV_ENDPOINT"), os.Getenv("DIREKTIV_NAMESPACE")), bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Add("direktiv-token", fmt.Sprintf("%s", strings.TrimSuffix(os.Getenv("DIREKTIV_TOKEN"), "\n")))
	req.Header.Add("Content-Type", "application/cloudevents+json; charset=utf-8")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// DirektivListener receives and processes a Cloud Audit Log event
func DirektivListener(w http.ResponseWriter, r *http.Request) {

	// read cloud event into a struct
	ce, err := readCloudEvent(r)
	if err != nil {
		log.Fatal(err)
		return
	}

	// log cloud event to container output
	err = logCloudEvent(ce)
	if err != nil {
		log.Fatal(err)
		return
	}

	// parse cloud event data to match spec
	event := parseCloudEvent(ce)

	// send cloud event to Direktiv
	data, err := sendCloudEvent(event)
	if err != nil {
		log.Fatal(err)
		return
	}

	log.Printf("Response of Direktiv: \n%s", data)
}

func main() {
	http.HandleFunc("/", DirektivListener)
	// Determine port for HTTP service.
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	// Start HTTP server.
	log.Printf("Listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}
