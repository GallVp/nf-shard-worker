package auth

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/99designs/gqlgen/graphql"
)

type contextKey struct {
	name string
}

var userCtxKey = &contextKey{"user"}

func AuthMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			headerParts := strings.Split(authHeader, " ")
			if len(headerParts) == 2 || headerParts[0] == "Bearer" {
				workerToken := headerParts[1]
				ctx := context.WithValue(r.Context(), userCtxKey, workerToken)
				r = r.WithContext(ctx)
			}

			next.ServeHTTP(w, r)
		})
	}
}

func Authorized() func(ctx context.Context, obj interface{}, next graphql.Resolver) (interface{}, error) {
	return func(ctx context.Context, obj interface{}, next graphql.Resolver) (interface{}, error) {
		appToken := os.Getenv("WORKER_TOKEN")
		userToken, ok := ctx.Value(userCtxKey).(string)
		if !ok {
			return nil, errors.New("access denied: invalid token")
		}

		if userToken != appToken {
			return nil, errors.New("access denied: invalid token")
		}

		return next(ctx)
	}
}
