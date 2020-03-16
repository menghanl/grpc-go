package client

type watchInfoSet struct {
	m map[*watchInfo]struct{}
}

func newWatchInfoSet() *watchInfoSet {
	return &watchInfoSet{m: make(map[*watchInfo]struct{})}
}

func (s *watchInfoSet) add(i *watchInfo) {
	s.m[i] = struct{}{}
}

func (s *watchInfoSet) has(i *watchInfo) bool {
	_, ok := s.m[i]
	return ok
}

func (s *watchInfoSet) forEach(f func(*watchInfo)) {
	for v := range s.m {
		f(v)
	}
}

func (s *watchInfoSet) remove(i *watchInfo) {
	delete(s.m, i)
}

func (s *watchInfoSet) len() int {
	return len(s.m)
}
