package api

// TooLargeAnswered returns how many 413s s has answered.
func TooLargeAnswered(s *Server) int64 { return s.tooLarge.Load() }

// InflightWriteBytes returns the write bytes s holds for requests in progress.
func InflightWriteBytes(s *Server) int64 { return s.writeBytes.used.Load() }
