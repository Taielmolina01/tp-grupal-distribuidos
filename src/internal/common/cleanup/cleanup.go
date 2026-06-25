package cleanup

import "log/slog"

type Closer interface{ Close() error }

func Close(closers ...Closer) {
	for _, c := range closers {
		if c != nil {
			if err := c.Close(); err != nil {
				slog.Error("While closing", "err", err)
			}
		}
	}
}
