package statemap

type StateMap[S any] struct {
	m     map[int]*S
	newFn func() *S
}

func New[S any](newFn func() *S) StateMap[S] {
	return StateMap[S]{m: make(map[int]*S), newFn: newFn}
}

func (sm *StateMap[S]) For(clientID int) *S {
	s, ok := sm.m[clientID]
	if !ok {
		s = sm.newFn()
		sm.m[clientID] = s
	}
	return s
}

func (sm *StateMap[S]) Set(clientID int, s *S) {
	sm.m[clientID] = s
}

func (sm *StateMap[S]) Delete(clientID int) {
	delete(sm.m, clientID)
}

func (sm *StateMap[S]) All() map[int]*S {
	return sm.m
}
