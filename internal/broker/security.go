package broker

import (
	"errors"
	"os"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"streamforge/internal/platform"
)

// SecurityOptions is shared by ingest, consumer, quarantine and replay clients.
func SecurityOptions(get func(string) string) ([]kgo.Opt, error) {
	enabled := false
	if raw := get("KAFKA_TLS"); raw != "" {
		var err error
		enabled, err = strconv.ParseBool(raw)
		if err != nil {
			return nil, errors.New("KAFKA_TLS must be boolean")
		}
	}
	ca, user, password := get("KAFKA_CA_PEM"), get("KAFKA_SASL_USERNAME"), get("KAFKA_SASL_PASSWORD")
	if !enabled && (ca != "" || user != "" || password != "") {
		return nil, errors.New("Kafka credentials/custom CA require TLS")
	}
	if (user == "") != (password == "") {
		return nil, errors.New("Kafka SASL requires both username and password")
	}
	var options []kgo.Opt
	if enabled {
		c, err := platform.TLSConfig(ca)
		if err != nil {
			return nil, err
		}
		options = append(options, kgo.DialTLSConfig(c))
	}
	if user != "" {
		options = append(options, kgo.SASL(scram.Auth{User: user, Pass: password}.AsSha512Mechanism()))
	}
	return options, nil
}
func NewClient(options ...kgo.Opt) (*kgo.Client, error) {
	security, err := SecurityOptions(os.Getenv)
	if err != nil {
		return nil, err
	}
	return kgo.NewClient(append(options, security...)...)
}
