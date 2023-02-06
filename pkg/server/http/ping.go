package httpserver

import (
	"net/http"
)

func pingHandler(res http.ResponseWriter, req *http.Request) {
	log.Info("request received at /ping")
	res.Header().Set("Content-Type", "text/plain; charset=utf-8")
	res.Write([]byte("pong"))
}
