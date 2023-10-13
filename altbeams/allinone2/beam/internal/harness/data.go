package harness

// Port represents the connection port of external operations.
type Port struct {
	URL string
}

// StreamID represents the static information needed to identify
// a data stream. Dynamic information, notably bundleID, is provided
// implicitly by the managers.
type StreamID struct {
	Port         Port
	PtransformID string
}

// Elements holds data or timers sent across the data channel.
// If TimerFamilyID is populated, it's a timer, otherwise it's
// data elements.
type Elements struct {
	Data, Timers                []byte
	TimerFamilyID, PtransformID string
}

// DataContext holds connectors to various data connections, incl. state and side input.
type DataContext struct {
	Data  *ScopedDataManager
	// State StateReader
}
