package store

// WithBlobChunkSize sets the blob chunk size in bytes, so tests can split a blob into
// many chunks without writing megabytes.
func WithBlobChunkSize(n int) Option { return func(o *options) { o.blobChunk = n } }
