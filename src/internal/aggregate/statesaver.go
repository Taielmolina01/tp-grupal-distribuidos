package aggregate

import (
	"os"
	"sync"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
)

type StateSaver interface {
	SaveState(
		accums map[int]map[string]partial,
		eofCounts map[int]int,
		eofTotals map[int]uint32,
	) error
	LoadState() (
		map[int]map[string]partial,
		map[int]int,
		map[int]uint32,
		error,
	)
}

type StateSaverImpl struct {
	filePath string
	mutex    *sync.Mutex
}

func CreateNewStateSaver(
	mutex *sync.Mutex,
	filePath string,
) StateSaver {
	return &StateSaverImpl{
		mutex:    mutex,
		filePath: filePath,
	}
}

func (s *StateSaverImpl) SaveState(
	accums map[int]map[string]partial,
	eofCounts map[int]int,
	eofTotals map[int]uint32,
) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	w := wire.NewWriter()

	w.Uint32(uint32(len(accums)))
	for clientID, methods := range accums {
		w.Int32(int32(clientID))
		w.Uint32(uint32(len(methods)))
		for method, p := range methods {
			w.String(method)
			w.Float64(p.totalSum)
			w.Int32(int32(p.totalCount))
		}
	}

	w.Uint32(uint32(len(eofCounts)))
	for clientID, count := range eofCounts {
		w.Int32(int32(clientID))
		w.Int32(int32(count))
		w.Uint32(eofTotals[clientID])
	}

	tmp := s.filePath + ".tmp"
	if err := os.WriteFile(tmp, w.Bytes(), 0644); err != nil {
		return err
	}
	return os.Rename(tmp, s.filePath)
}

func (s *StateSaverImpl) LoadState() (map[int]map[string]partial, map[int]int, map[int]uint32, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	data, err := os.ReadFile(s.filePath)
	if os.IsNotExist(err) {
		return nil, nil, nil, err
	}
	if err != nil {
		return nil, nil, nil, err
	}

	r := wire.NewReader(data)

	numClients := r.Uint32()
	accums := make(map[int]map[string]partial, numClients)
	for range numClients {
		clientID := int(r.Int32())
		numMethods := r.Uint32()
		methods := make(map[string]partial, numMethods)
		for range numMethods {
			method := r.String()
			totalSum := r.Float64()
			totalCount := int(r.Int32())
			methods[method] = partial{totalSum: totalSum, totalCount: totalCount}
		}
		if r.Err() != nil {
			return nil, nil, nil, r.Err()
		}
		accums[clientID] = methods
	}

	numEOFs := r.Uint32()
	eofCounts := make(map[int]int, numEOFs)
	eofTotals := make(map[int]uint32, numEOFs)
	for range numEOFs {
		clientID := int(r.Int32())
		count := int(r.Int32())
		total := r.Uint32()
		eofCounts[clientID] = count
		eofTotals[clientID] = total
	}

	return accums, eofCounts, eofTotals, r.Err()
}
