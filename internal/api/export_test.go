package api

// TooLargeAnswered returns how many 413s s has answered.
func TooLargeAnswered(s *Server) int64 { return s.tooLarge.Load() }
