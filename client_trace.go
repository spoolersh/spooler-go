package spooler

//go:generate gtrace

import "log/slog"

//gtrace:gen
type ClientTrace struct {
}

func LoggerClientTrace(logger *slog.Logger) ClientTrace {
	return ClientTrace{}
}
