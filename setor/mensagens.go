package main

// Structs do tipo Mensagem

// Identifica Tipo do remetente - servico, recurso, drone ou broker
type base struct {
	Tipo string `json:"tipo"`
	ID   string `json:"id"`
}

// -------- Broker <-> Broker --------
type MensagemBroker struct {
	Tipo  string `json:"tipo"` // "broker"
	ID    string `json:"id"`
	Reply string `json:"reply"` // "REQUEST" | "OK"

	Requisicao
}

// -------- Drone --------
type MensagemDrone struct {
	Tipo  string `json:"tipo"` // "drone"
	ID    string `json:"id"`
	Acao  string `json:"acao"` // requisicao | andamento | conclusao
	Sinal bool   `json:"sinal"`
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
