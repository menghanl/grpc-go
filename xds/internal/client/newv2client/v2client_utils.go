package client

type stringSet struct {
	m map[string]struct{}
}

func newStringSet() *stringSet {
	return &stringSet{m: make(map[string]struct{})}
}

func (s *stringSet) add(i string) {
	s.m[i] = struct{}{}
}

func (s *stringSet) remove(i string) {
	delete(s.m, i)
}

func (s *stringSet) len() int {
	return len(s.m)
}

func (s *stringSet) toSlice() (ret []string) {
	for i := range s.m {
		ret = append(ret, i)
	}
	return
}

// func (s *stringSet) has(i string) bool {
// 	_, ok := s.m[i]
// 	return ok
// }
//
// func (s *stringSet) forEach(f func(string)) {
// 	for v := range s.m {
// 		f(v)
// 	}
// }
