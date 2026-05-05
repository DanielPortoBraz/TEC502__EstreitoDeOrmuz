package main

// Structs do tipo Mensagem

// Identifica Tipo do remetente - servico, recurso, drone ou broker
type base struct {
	Tipo string `json:"tipo"`
	ID   string `json:"id"`
	Timestamp int64 `json:"timestamp"`
}

// -------- Broker <-> Broker --------
type MensagemBroker struct {
	Tipo  string `json:"tipo"` // "broker"
	ID    string `json:"id"`
	Reply string `json:"reply"` // "REQUEST" | "OK"
	Timestamp int64  `json:"timestamp"`

	Requisicao
}

// -------- Drone --------
type MensagemDrone struct {
	Tipo  string `json:"tipo"`
	ID    string `json:"id"`
	Acao  string `json:"acao"` 
	Sinal bool   `json:"sinal"`
	Estado string  `json:"estado"`
	Timestamp int64 `json:"timestamp"`
}

// -------- Servico --------
type MensagemServico struct {
	Tipo       string  `json:"tipo"`
	ID         string  `json:"id"`
	Timestamp  int64   `json:"timestamp"`
	Prioridade int64 `json:"prioridade"`
}

// -------- Recurso --------
type MensagemRecurso struct {
	Tipo       string  `json:"tipo"`
	ID         string  `json:"id"`
	Timestamp  int64   `json:"timestamp"`
	Prioridade int64 `json:"prioridade"`
}
