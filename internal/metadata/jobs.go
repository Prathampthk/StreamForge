package metadata

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

type Job struct {
	conn *pgxpool.Conn
	ID   string
}

func (s *Store) LockJob(ctx context.Context, id, tenant, actor, source string, selector []byte) (*Job, error) {
	conn, err := s.Pool.Acquire(ctx)
	if err != nil {
		return nil, errors.New("acquire replay audit connection")
	}
	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1,0))`, id).Scan(&locked); err != nil || !locked {
		conn.Release()
		return nil, errors.New("replay job is locked or metadata unavailable")
	}
	job := &Job{conn: conn, ID: id}
	fail := func() (*Job, error) { job.Close(); return nil, errors.New("create/validate immutable replay job") }
	if _, err := conn.Exec(ctx, `INSERT INTO tenants(tenant_id,name) VALUES($1,$1) ON CONFLICT DO NOTHING`, tenant); err != nil {
		return fail()
	}
	if _, err := conn.Exec(ctx, `INSERT INTO replay_jobs(job_id,tenant_id,source_topic,status,selector,requested_by) VALUES($1,$2,$3,'pending',$4,$5) ON CONFLICT DO NOTHING`, id, tenant, source, selector, actor); err != nil {
		return fail()
	}
	var matches bool
	if err := conn.QueryRow(ctx, `SELECT tenant_id=$2 AND source_topic=$3 AND selector=$4::jsonb AND requested_by=$5 FROM replay_jobs WHERE job_id=$1`, id, tenant, source, selector, actor).Scan(&matches); err != nil || !matches {
		return fail()
	}
	if _, err := conn.Exec(ctx, `UPDATE replay_jobs SET status='running',last_error=NULL,updated_at=now() WHERE job_id=$1`, id); err != nil {
		return fail()
	}
	return job, nil
}
func (j *Job) AlreadyPublished(ctx context.Context, topic string, partition int32, offset int64) (bool, error) {
	var exists bool
	err := j.conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM replay_audit WHERE job_id=$1 AND source_topic=$2 AND source_partition=$3 AND source_offset=$4 AND outcome='republished')`, j.ID, topic, partition, offset).Scan(&exists)
	return exists, err
}
func (j *Job) Audit(ctx context.Context, topic string, partition int32, offset int64, eventID, outcome string) error {
	_, err := j.conn.Exec(ctx, `INSERT INTO replay_audit(job_id,source_topic,source_partition,source_offset,event_id,outcome) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(job_id,source_topic,source_partition,source_offset) DO UPDATE SET outcome=EXCLUDED.outcome`, j.ID, topic, partition, offset, eventID, outcome)
	return err
}
func (j *Job) Finish(ctx context.Context, status string, scanned int64, message string) error {
	_, err := j.conn.Exec(ctx, `UPDATE replay_jobs SET status=$2,records_scanned=$3,records_republished=(SELECT count(*) FROM replay_audit WHERE job_id=$1 AND outcome='republished'),last_error=NULLIF($4,''),updated_at=now() WHERE job_id=$1`, j.ID, status, scanned, message)
	return err
}
func (j *Job) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := j.conn.Exec(ctx, `SELECT pg_advisory_unlock(hashtextextended($1,0))`, j.ID); err != nil {
		j.conn.Conn().Close(ctx)
	}
	j.conn.Release()
}

// Status reports persisted audit progress without starting/resuming a job.
func (s *Store) Status(ctx context.Context, id string) (map[string]any, error) {
	var tenant, actor, status string
	var scanned, published int64
	var lastError *string
	err := s.Pool.QueryRow(ctx, `SELECT tenant_id,requested_by,status,records_scanned,records_republished,last_error FROM replay_jobs WHERE job_id=$1`, id).Scan(&tenant, &actor, &status, &scanned, &published, &lastError)
	if err != nil {
		return nil, errors.New("replay job unavailable")
	}
	return map[string]any{"job_id": id, "tenant_id": tenant, "requested_by": actor, "status": status, "records_scanned": scanned, "records_republished": published, "last_error": lastError}, nil
}
