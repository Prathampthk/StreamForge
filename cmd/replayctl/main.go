package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"io"
	"os"
	"streamforge/internal/domain"
	"streamforge/internal/metadata"
	"streamforge/internal/replay"
	"strings"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "replay refused/failed:", err)
		os.Exit(1)
	}
}
func run() error {
	o := replay.Options{SourceTopic: replay.DLQTopic, TargetTopic: "streamforge.events.v1", ByteLimit: 8 << 20}
	statusOnly := flag.Bool("status", false, "read persisted job status; requires --job-id")
	partition := flag.Int("partition", -1, "DLQ partition 0..11")
	flag.Int64Var(&o.From, "from", -1, "inclusive DLQ offset")
	flag.Int64Var(&o.To, "to", -1, "exclusive DLQ offset (<=1000 offsets)")
	flag.StringVar(&o.Tenant, "tenant", "", "required tenant scope")
	flag.StringVar(&o.Actor, "requested-by", "", "required local operator identity")
	flag.StringVar(&o.JobID, "job-id", uuid.NewString(), "immutable job UUID; reuse to resume same selector")
	flag.BoolVar(&o.Execute, "execute", false, "publish; default is audited dry-run")
	repairFile := flag.String("repair-file", "", "optional bounded JSON map envelope_id -> repaired event")
	flag.Parse()
	o.Partition = int32(*partition)
	if *statusOnly {
		if parsed, err := uuid.Parse(o.JobID); err != nil || parsed.String() != o.JobID {
			return errors.New("canonical job UUID required")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		store, err := metadata.FromEnv(ctx)
		if err != nil {
			return err
		}
		defer store.Close()
		status, err := store.Status(ctx, o.JobID)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(status)
	}
	if flag.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *repairFile != "" {
		file, err := os.Open(*repairFile)
		if err != nil {
			return errors.New("cannot open repair file")
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil || info.Size() > 1<<20 {
			return errors.New("repair file exceeds 1 MiB")
		}
		raw, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
		if err != nil || len(raw) > 1<<20 {
			return errors.New("repair file exceeds bound")
		}
		if err := domain.CheckJSON(raw); err != nil {
			return errors.New("malformed or duplicate-key repair JSON")
		}
		if err := json.Unmarshal(raw, &o.Repairs); err != nil {
			return errors.New("invalid repair JSON")
		}
		for key := range o.Repairs {
			if _, err := uuid.Parse(key); err != nil {
				return errors.New("repair keys must be envelope UUIDs")
			}
		}
	}
	if err := o.Validate(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	store, err := metadata.FromEnv(ctx)
	if err != nil {
		return err
	}
	defer store.Close()
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	result, err := (replay.Runner{Brokers: strings.Split(brokers, ","), Store: store}).Run(ctx, o)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
