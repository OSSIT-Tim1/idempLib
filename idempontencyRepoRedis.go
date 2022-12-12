package idempLib

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"os"
	"time"
)

type idempontencyRepoRedis struct {
	cli    *redis.Client
	Tracer trace.Tracer
}

type IdempontencyRepo interface {
	Exists(id string, ctx context.Context) bool
	Save(id string, ctx context.Context) (string, error)
}

const (
	reqKey = "req:%s"
)

/*
NewIdempotenceRepo generates new instance of idempontency repo and takes in tracer as optional parameter.
Function also returns tooManyArgumentsErr if we pass in more than one tracer as a parameter.
Error is thrown if IDEMPOTENCE_REDIS_HOST or IDEMPOTENCE_REDIS_PORT variables are not find in .env
*/
func NewIdempotenceRepo(tracer ...trace.Tracer) (IdempontencyRepo, error) {

	if len(tracer) > 1 {
		return nil, tooManyArgumentsError("NewIdempotenceRepo")
	}

	if len(tracer) == 0 {
		tracer = make([]trace.Tracer, 1)
	}

	host := os.Getenv("IDEMPOTENCE_REDIS_HOST")
	port := os.Getenv("IDEMPOTENCE_REDIS_PORT")

	if host == "" || port == "" {
		return nil, errors.New("couldn't read .env variables for IDEMPOTENCE_REDIS_HOST,IDEMPOTENCE_REDIS_PORT. Please check if you provided them correctly")
	}
	adr := fmt.Sprintf("%s:%s", host, port)

	client := redis.NewClient(&redis.Options{
		Addr: adr,
	})

	return idempontencyRepoRedis{
		cli:    client,
		Tracer: tracer[0],
	}, nil
}

/*
Exists checks if provided id exists in db and returns bool as response
*/
func (i idempontencyRepoRedis) Exists(id string, ctx context.Context) bool {
	if i.Tracer != nil {
		_, span := i.Tracer.Start(ctx, "IdempontencyRepoRedis.Exists")
		defer span.End()
	}

	return i.cli.Exists(constructKey(id)).Val() == 1
}

/*
Save stores provided id in db with TTL : 3min
*/
func (i idempontencyRepoRedis) Save(id string, ctx context.Context) (string, error) {
	key := constructKey(id)

	if i.Tracer != nil {
		_, span := i.Tracer.Start(ctx, "IdempontencyRepoRedis.Save")
		defer span.End()

		err := i.cli.Set(key, true, time.Duration(3)*time.Minute).Err()
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return "", err
		}
	} else {
		err := i.cli.Set(key, true, time.Duration(3)*time.Minute).Err()
		if err != nil {
			return "", err
		}
	}
	return id, nil
}

/*
constructKey constructs our db key based on UUID(as a string) that is passed in as a parameter
*/
func constructKey(id string) string {
	return fmt.Sprintf(reqKey, id)
}
