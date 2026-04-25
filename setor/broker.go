// Broker: representa a central de gerenciamento do setor, recebendo dados dos servicos/recursos e enviando requisições para os drones

// ----------- Structs ----------
// Requisição: contém prioridade e timestamp

// Broker: contém maps[Conn, mensagem] de brokers, servicos, recursos e drones; e a fila de requisições do seu próprio setor
// ----------- Lógica -------
/*
1. Inicializa Broker
2. Inicia servidor TCP e conecta cada dispositivo ao seu map respectivamente
3. Aguarda mensagens, que podem ser de :
	3.1. Dados (ações critícas ou comuns)
		3.1.1. Caso seja ação crítica, envia requisição para a fila
	3.2. Confirmação de Brokers (OK)
	3.3. Sinal de conclusão dado pelo drone ou requisição de maior prioridade ou perca de conexão
*/

/* ----------- Funções -----------
1. iniciaServidorTCP()
2. handleTCP(...): apenas gerencia as conexões TCP recebendo mensagens. Separa mensagens pela função dispatcher (passo seguinte)
3. dispatcher(...): separa as mensagens conforme o tipo (uso de channels), chamando uma função que fará a sua tratativa. Os tipos de mensagens são: servico, recurso, drone, broker
  4.1 handleServico(...): trata as mensagens de servico, adicionando uma requisição a fila caso o valor esteja acima de 70.
  4.2 handleRecurso(...): mesmo do servico
  4.3 handleBroker(...): associa mensagem dos brokers no map
  4.4 handleDrone(...): ao ter um item na fila de requisições, envia solicitação de drone (pergunta aos outros Brokers), verifica se todos brokers 
						do map aprovam a solicitação, e caso aprovem, envia mensagem para que drone venha até ele (dispara requisição para o map de drones), 
						a conexão será mantida até ocorrer o sinal de conclusão do drone ou uma requisição de maior prioridade ou uma perca de conexão 
*/

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

// ----------- Structs ----------

// Identifica Tipo da origem - servico, recurso, drone ou broker
var base struct {
		Tipo string `json:"tipo"`
		ID   string `json:"id"`
}

type Requisicao struct {
	Prioridade float64 `json:"prioridade"`
	Timestamp  int64   `json:"timestamp"`
	Origem     string  `json:"origem"`
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
	Acao  string `json:"acao"` // andamento | conclusao
	Sinal bool   `json:"sinal"`
}

// -------- Servico --------
type MensagemServico struct {
	Tipo      string  `json:"tipo"`
	ID        string  `json:"id"`
	Timestamp int64   `json:"timestamp"`
	Valor     float64 `json:"valor"`
}

// -------- Recurso --------
type MensagemRecurso struct {
	Tipo      string  `json:"tipo"`
	ID        string  `json:"id"`
	Timestamp int64   `json:"timestamp"`
	Valor     float64 `json:"valor"`
}

// -------- Broker --------
type Broker struct {
	mu sync.Mutex

	id string

	brokers  map[net.Conn]string
	servicos map[net.Conn]string
	recursos map[net.Conn]string
	drones   map[net.Conn]string

	fila []Requisicao

	// canais (dispatcher)
	chServico chan MensagemServico
	chRecurso chan MensagemRecurso
	chDrone   chan MensagemDrone
	chBroker  chan MensagemBroker
}

// ----------- Inicialização ----------

func novoBroker(id string) *Broker {
	return &Broker{
		id: id,

		brokers:  make(map[net.Conn]string),
		servicos: make(map[net.Conn]string),
		recursos: make(map[net.Conn]string),
		drones:   make(map[net.Conn]string),

		fila: []Requisicao{},

		chServico: make(chan MensagemServico, 100),
		chRecurso: make(chan MensagemRecurso, 100),
		chDrone:   make(chan MensagemDrone, 100),
		chBroker:  make(chan MensagemBroker, 100),
	}
}

// ----------- Servidor TCP ----------

func (b *Broker) iniciaServidorTCP(porta string) {
	listener, err := net.Listen("tcp", ":"+porta)
	if err != nil {
		panic(err)
	}

	fmt.Println("Broker", b.id, "rodando na porta", porta)

	go b.dispatcher()

	for {
		conn, _ := listener.Accept()
		fmt.Println("Novo dispotivo conectado:", conn.RemoteAddr())
		go b.handleTCP(conn)
	}
}

// ----------- Handle TCP ----------

func (b *Broker) handleTCP(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Lê primeira mensagem (handshake)
	msgStr, err := reader.ReadString('\n')
	if err != nil {
		b.removerConexao(conn)
		return
	}

	err = json.Unmarshal([]byte(msgStr), &base)
	if err != nil {
		fmt.Println("Erro no handshake")
		return
	}

	// Registra nas conexões (maps)
	b.registrar(conn, base.Tipo, base.ID)

	// Loop dispatcher
	for {
		msgStr, err := reader.ReadString('\n')
		if err != nil {
			b.removerConexao(conn)
			return
		}

		b.dispatch(conn, []byte(msgStr))
	}
}

// Registrar Conexão (associa cada conexão ao map do seu respectivo tipo)
func (b *Broker) registrar(conn net.Conn, tipo string, id string) {

	b.mu.Lock()
	defer b.mu.Unlock()

	switch tipo {

	case "servico":
		if _, ok := b.servicos[conn]; !ok {
			b.servicos[conn] = id
			fmt.Println("Serviço registrado:", id)
		}

	case "recurso":
		if _, ok := b.recursos[conn]; !ok {
			b.recursos[conn] = id
			fmt.Println("Recurso registrado:", id)
		}

	case "drone":
		if _, ok := b.drones[conn]; !ok {
			b.drones[conn] = id
			fmt.Println("Drone registrado:", id)
		}

	case "broker":
		if _, ok := b.brokers[conn]; !ok {
			b.brokers[conn] = id
			fmt.Println("Broker registrado:", id)
		}
	}
}

// ----------- Dispatcher ----------
func (b *Broker) dispatch(conn net.Conn, data []byte) {

	json.Unmarshal(data, &base)

	switch base.Tipo {

	case "servico":
		var m MensagemServico
		json.Unmarshal(data, &m)
		b.chServico <- m

	case "recurso":
		var m MensagemRecurso
		json.Unmarshal(data, &m)
		b.chRecurso <- m

	case "drone":
		var m MensagemDrone
		json.Unmarshal(data, &m)
		b.chDrone <- m

	case "broker":
		var m MensagemBroker
		json.Unmarshal(data, &m)
		b.chBroker <- m
	}
}

// ----------- Loop central ----------

func (b *Broker) dispatcher() {
	for {
		select {

		case msg := <-b.chServico:
			b.handleServico(msg)

		case msg := <-b.chRecurso:
			b.handleRecurso(msg)

		case msg := <-b.chDrone:
			b.handleDrone(msg)

		case msg := <-b.chBroker:
			b.handleBroker(msg)
		}
	}
}

// ----------- Handlers ----------

// Serviço
func (b *Broker) handleServico(msg MensagemServico) {

	if msg.Valor > 70 {
		req := Requisicao{
			Prioridade: msg.Valor,
			Timestamp:  time.Now().UnixNano(),
			Origem:     msg.ID,
		}

		b.adicionarFila(req)
	}
}

// Recurso
func (b *Broker) handleRecurso(msg MensagemRecurso) {

	if msg.Valor > 70 {
		req := Requisicao{
			Prioridade: msg.Valor,
			Timestamp:  time.Now().UnixNano(),
			Origem:     msg.ID,
		}

		b.adicionarFila(req)
	}
}

// Broker (base Agrawala)
func (b *Broker) handleBroker(msg MensagemBroker) {

	switch msg.Reply {

	case "REQUEST":
		fmt.Println("REQUEST recebido de", msg.ID)

		// TODO: comparar prioridade (Agrawala)
		// por enquanto responde OK direto

		resp := MensagemBroker{
			Tipo:  "broker",
			ID:    b.id,
			Reply: "OK",
		}

		b.broadcastBroker(resp)

	case "OK":
		fmt.Println("OK recebido de", msg.ID)
		// TODO: contabilizar confirmações
	}
}

// Drone
func (b *Broker) handleDrone(msg MensagemDrone) {

	if msg.Sinal { // Se o drone deu o sinal de conclusão, segue para a próxima requisição
		fmt.Println("Drone concluiu:", msg.ID)

		b.mu.Lock()
		if len(b.fila) > 0 {
			b.fila = b.fila[1:]
		}
		b.mu.Unlock()

		// tenta próxima
		go b.tentarDespachar()

	} else {
		fmt.Printf("Drone %s em execução: %s\n", msg.ID, msg.Acao)
	}
}

func (b *Broker) enviarParaDrone(conn net.Conn) {

	msg := MensagemDrone{
		Tipo:  "drone",
		ID:    b.id,
		Acao:  "requisicao",
		Sinal: false,
	}

	data, _ := json.Marshal(msg)
	conn.Write(append(data, '\n'))
}

func (b *Broker) escolherDrone() net.Conn {

	b.mu.Lock()
	defer b.mu.Unlock()

	for conn := range b.drones {
		return conn // pega o primeiro drone disponível
	}

	return nil
}

// ----------- Fila ----------

func (b *Broker) adicionarFila(req Requisicao) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.fila = append(b.fila, req)

	fmt.Println("Fila:")

	for i := range b.fila{
		fmt.Printf("%d - [\nID: %s\nPrioridade: %.2f\nTimeStamp:%d\n]\n", i, b.fila[i].Origem, b.fila[i].Prioridade, b.fila[i].Timestamp)
	}

	// dispara tentativa de uso de drone
	go b.tentarDespachar()
}


// ----------- Agrawala (início) ----------

func (b *Broker) tentarDespachar() {

	b.mu.Lock()
	if len(b.fila) == 0 {
		b.mu.Unlock()
		return
	}
	b.mu.Unlock()

	droneConn := b.escolherDrone()

	if droneConn == nil {
		fmt.Println("Nenhum drone disponível")
		return
	}

	fmt.Println("Enviando requisição para drone")

	b.enviarParaDrone(droneConn)
}

// broadcast
func (b *Broker) broadcastBroker(msg MensagemBroker) {

	data, _ := json.Marshal(msg)

	for conn := range b.brokers {
		conn.Write(append(data, '\n'))
	}
}

// ----------- Conexões ----------

func (b *Broker) removerConexao(conn net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.brokers, conn)
	delete(b.servicos, conn)
	delete(b.recursos, conn)
	delete(b.drones, conn)
}

// ----------- Main ----------

func main() {
	broker := novoBroker("B1")
	broker.iniciaServidorTCP("8080")
}