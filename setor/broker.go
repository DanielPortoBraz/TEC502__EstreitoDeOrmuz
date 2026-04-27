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
	"os"
	"sync"
	"time"
)

// ----------- Structs ----------

// Identifica Tipo da origem - servico, recurso, drone ou broker
type base struct {
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

	// --- Estado Ricart-Agrawala ---
	requesting bool       // true enquanto aguarda OKs dos peers
	inCS       bool       // true enquanto usa o drone (região crítica)
	currentReq Requisicao // requisição que este broker está disputando no momento
	okCount    int        // OKs recebidos no round atual
	deferred   []net.Conn // conns de peers cujo OK foi adiado; receberão OK após sair da CS

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

	// canal broadcast
	chBroadcast chan MensagemBroker
}

// ----------- Inicialização ----------

func novoBroker(id string) *Broker {
	b := &Broker{
		id: id,

		brokers:  make(map[net.Conn]string),
		servicos: make(map[net.Conn]string),
		recursos: make(map[net.Conn]string),
		drones:   make(map[net.Conn]string),

		fila: []Requisicao{},

		chServico:   make(chan MensagemServico, 100),
		chRecurso:   make(chan MensagemRecurso, 100),
		chDrone:     make(chan MensagemDrone, 100),
		chBroker:    make(chan MensagemBroker, 100),
		chBroadcast: make(chan MensagemBroker, 100),
	}

	return b
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

	base := base{}
	err = json.Unmarshal([]byte(msgStr), &base)
	if err != nil {
		fmt.Println("Erro no handshake")
		return
	}

	// Registra a conn no map apenas se não for um broker peer.
	// Para brokers: o lado CLIENTE (handlePeer) já registrou a conn de escrita.
	// O lado servidor só precisa ler e despachar — não registra uma segunda conn.
	if base.Tipo != "broker" {
		b.registrar(conn, base.Tipo, base.ID)
	}

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

	base := base{}
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

// ----------- Ricart-Agrawala ----------

// temPrioridade retorna true se a requisição `a` tem prioridade MAIOR que `outro`
// (ou seja: `a` deve entrar na CS antes de `outro`)
func (b *Broker) temPrioridade(a, outro Requisicao) bool {
	if a.Prioridade != outro.Prioridade {
		return a.Prioridade > outro.Prioridade // maior valor de prioridade vence
	}
	if a.Timestamp != outro.Timestamp {
		return a.Timestamp < outro.Timestamp // menor timestamp (chegou antes) vence
	}
	return a.Origem < outro.Origem // desempate determinístico pelo ID
}

// enviarOK manda OK diretamente para uma conn específica (sem broadcast)
func (b *Broker) enviarOK(conn net.Conn) {
	resp := MensagemBroker{
		Tipo:  "broker",
		ID:    b.id,
		Reply: "OK",
	}
	data, _ := json.Marshal(resp)
	conn.Write(append(data, '\n'))
}

// handleBroker implementa a lógica de recepção do Ricart-Agrawala.
//
// Para o caso de 2 brokers: o map b.brokers tem exatamente 1 conn (o peer),
// então broadcast == resposta direta. O deferred também guarda essa conn.
// Para N brokers no futuro: trocar chBroker por um wrapper {conn, msg}.
func (b *Broker) handleBroker(msg MensagemBroker) {

	switch msg.Reply {

	case "REQUEST":
		fmt.Println("REQUEST recebido de", msg.ID)

		b.mu.Lock()

		// Regra do Ricart-Agrawala:
		//   - Não estou pedindo nem na CS → cedo passagem imediatamente
		//   - Estou na CS → adio (o peer espera eu terminar)
		//   - Ambos pedindo → quem tem maior prioridade entra; o outro adia
		var cederPassagem bool
		if b.inCS {
			cederPassagem = false
		} else if !b.requesting {
			cederPassagem = true
		} else {
			// Ambos competindo: cedo se o peer tem maior prioridade que eu
			cederPassagem = b.temPrioridade(msg.Requisicao, b.currentReq)
		}

		if cederPassagem {
			b.mu.Unlock()
			// Para 2 brokers: broadcast chega exatamente no peer que pediu
			b.chBroadcast <- MensagemBroker{Tipo: "broker", ID: b.id, Reply: "OK"}
		} else {
			// Guarda a conn do peer para enviar OK depois de sair da CS.
			// Para 2 brokers: há exatamente 1 conn em b.brokers.
			for conn := range b.brokers {
				b.deferred = append(b.deferred, conn)
			}
			b.mu.Unlock()
			fmt.Println("OK adiado para", msg.ID)
		}

	case "OK":
		b.mu.Lock()
		b.okCount++
		total := len(b.brokers)
		fmt.Printf("OK recebido de %s | %d/%d\n", msg.ID, b.okCount, total)

		// Recebeu OK de todos os peers → entra na CS
		if b.okCount >= total {
			b.mu.Unlock()
			b.entrarCS()
		} else {
			b.mu.Unlock()
		}
	}
}

// entrarCS: entra na região crítica e despacha o drone
func (b *Broker) entrarCS() {
	b.mu.Lock()
	b.inCS = true
	b.requesting = false
	b.mu.Unlock()

	fmt.Println(">>> Entrando na REGIÃO CRÍTICA (usando drone)")

	droneConn := b.escolherDrone()
	if droneConn != nil {
		b.enviarParaDrone(droneConn)
	}
}

// Drone
func (b *Broker) handleDrone(msg MensagemDrone) {

	if msg.Sinal { // sinal de conclusão → sai da CS
		fmt.Println("Drone concluiu:", msg.ID)

		b.mu.Lock()

		if len(b.fila) > 0 {
			b.fila = b.fila[1:]
		}

		b.inCS = false

		// Captura OKs adiados e limpa a lista com o lock ainda ativo
		pendentes := b.deferred
		b.deferred = nil

		b.mu.Unlock()

		// Envia OKs adiados diretamente às conns dos peers que esperavam
		for _, conn := range pendentes {
			fmt.Println("Enviando OK adiado para peer")
			b.enviarOK(conn)
		}

		// Tenta atender próxima requisição da fila
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
		return conn
	}

	return nil
}

// ----------- Fila ----------

func (b *Broker) adicionarFila(req Requisicao) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.fila = append(b.fila, req)

	fmt.Println("Fila:")

	for i := range b.fila {
		fmt.Printf("%d - [\nID: %s\nPrioridade: %.2f\nTimeStamp:%d\n]\n", i, b.fila[i].Origem, b.fila[i].Prioridade, b.fila[i].Timestamp)
	}

	// dispara tentativa de uso de drone
	go b.tentarDespachar()
}

// ----------- Agrawala (início) ----------

func (b *Broker) tentarDespachar() {

	b.mu.Lock()

	// Não faz nada se: fila vazia, já estamos pedindo, ou já estamos na CS
	if len(b.fila) == 0 || b.requesting || b.inCS {
		b.mu.Unlock()
		return
	}

	// Marca que estamos pedindo acesso
	b.requesting = true
	b.okCount = 0
	b.currentReq = b.fila[0]
	totalPeers := len(b.brokers)
	req := b.currentReq

	b.mu.Unlock()

	// Sem peers: entra direto na CS (não há ninguém para pedir permissão)
	if totalPeers == 0 {
		b.entrarCS()
		return
	}

	// Envia REQUEST para todos os peers
	msg := MensagemBroker{
		Tipo:       "broker",
		ID:         b.id,
		Reply:      "REQUEST",
		Requisicao: req,
	}

	b.chBroadcast <- msg
	fmt.Println("REQUEST enviado para brokers")
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

// Comunicação com outro Broker (lado cliente)
//
// Faz o handshake, registra a conn e fica em loop consumindo chBroadcast —
// enviando cada mensagem diretamente ao peer. 
func (b *Broker) handlePeer(address string) {
	var conn net.Conn

	// Tenta conectar até conseguir
	for {
		c, err := net.Dial("tcp", ":"+address)
		if err == nil {
			conn = c
			break
		}
		fmt.Println("[Broker] Tentando conectar a", address, "...")
		time.Sleep(2 * time.Second)
	}

	fmt.Println("[Broker] Conectado a", address)

	// Envia handshake de identificação
	data, _ := json.Marshal(MensagemBroker{Tipo: "broker", ID: b.id})
	conn.Write(append(data, '\n'))

	// Registra a conn no map de brokers
	b.registrar(conn, "broker", address)

	// Loop de envio: consome chBroadcast e escreve na conn do peer
	for msg := range b.chBroadcast {
		payload, _ := json.Marshal(msg)
		_, err := conn.Write(append(payload, '\n'))
		if err != nil {
			fmt.Println("[Broker] Erro ao enviar para peer:", err)
			b.removerConexao(conn)
			return
		}
	}
}

// ----------- Main ----------

func main() {
	// Ao executar o programa, espera-se os argumentos: go run broker.go <ID Broker> <Broker Port> <BrokerPeer Port>
	broker := novoBroker(os.Args[1])
	go broker.handlePeer(os.Args[3])
	broker.iniciaServidorTCP(os.Args[2])
}