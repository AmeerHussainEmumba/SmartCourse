package identity

// SetReseedBatchForTest shrinks the re-seed page size so pagination is
// exercised with a handful of users.
func (s *Service) SetReseedBatchForTest(n int) { s.reseedBatch = n }
