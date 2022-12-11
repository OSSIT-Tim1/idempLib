package idempLib

import (
	"errors"
	"fmt"
	"go.opentelemetry.io/otel/trace"
	"net/http"
)

const (
	IDEMP_HEDER = "Idempotency-Key"
)

func tooManyArgumentsError(fnc string) error {
	return errors.New(fmt.Sprintf("%s : you passed in to many argumants into this function", fnc))
}

type IdempotencyHandler interface {
	MiddlewareIdempotency(next http.Handler) http.Handler
}

type IdempotencyHandlerImpl struct {
	repo   IdempontencyRepo
	Tracer trace.Tracer
}

/*
NewIdempotencyService generates new instance of idempontency service and takes in tracer as optional parameter.
Function also returns tooManyArgumentsErr if we pass in more than one tracer as a parameter.
*/
func NewIdempotencyHandler(tracer ...trace.Tracer) (IdempotencyHandler, error) {
	if len(tracer) > 1 {
		return nil, tooManyArgumentsError("NewIdempotencyService")
	}

	repo, err := NewIdempotenceRepo(tracer[0])
	if err != nil {
		return nil, err
	}

	return &IdempotencyHandlerImpl{
		repo:   repo,
		Tracer: tracer[0],
	}, nil
}

/*
MiddlewareIdempotency is middleware function which intercepts all incoming requests. Function check if request can collapse consistency of our system(PUT,POST,DELETE,PUT)
and checks header for Idempotency-key variable to see if that request was handled before and stored in db. If not it will store it in redis with TLL = 3min
*/
func (service *IdempotencyHandlerImpl) MiddlewareIdempotency(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, h *http.Request) {
		if h.Method == http.MethodPost || h.Method == http.MethodPut || h.Method == http.MethodPatch || h.Method == http.MethodDelete {
			ctx, span := service.Tracer.Start(h.Context(), "IdempotencyHandler.MiddlewareIdempotency")
			defer span.End()
			if h.Header.Get(IDEMP_HEDER) != "" && service.repo.Exists(h.Header.Get(IDEMP_HEDER), ctx) {
				rw.WriteHeader(http.StatusOK)
				return
			} else {
				service.repo.Save(h.Header.Get(IDEMP_HEDER), ctx)
			}
		}
		next.ServeHTTP(rw, h)
	})
}