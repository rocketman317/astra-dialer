package astra

// Endpoint is a struct that holds the AstraDB CQL endpoint information: SNI address and internal node IDs.
type Endpoint struct {
	SniAddress    string   `json:"sniAddress"`
	ContactPoints []string `json:"contactPoint"`
	Region        string   `json:"region"`
}
