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

	fila FilaPrioridade

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

		fila: FilaPrioridade{},

		chServico:   make(chan MensagemServico, 100),
		chRecurso:   make(chan MensagemRecurso, 100),
		chDrone:     make(chan MensagemDrone, 100),
		chBroker:    make(chan MensagemBroker, 100),
		chBroadcast: make(chan MensagemBroker, 100),
	}

	fmt.Printf("(%s) [Broker %s] - [INIT]: Broker criado\n", timeStamp(), id)

	return b
}

// ----------- Servidor TCP ----------

func (b *Broker) iniciaServidorTCP(porta string) {
	listener, err := net.Listen("tcp", ":"+porta)
	if err != nil {
		panic(err)
	}

	fmt.Printf("(%s) [Broker %s] - [INIT]: rodando na porta %s\n", timeStamp(), b.id, porta)

	go b.dispatcher()

	for {
		conn, _ := listener.Accept()
		fmt.Printf("(%s) [Broker %s] - [TCP]: novo dispositivo conectado %s\n", timeStamp(), b.id, conn.RemoteAddr())
		go b.handleTCP(conn)
	}
}

// ----------- Handler Conexão ----------

func (b *Broker) handleTCP(conn net.Conn) {
	defer conn.Close()

	fmt.Printf("(%s) [Broker %s] - [TCP]: iniciando conexão %s\n", timeStamp(), b.id, conn.RemoteAddr())

	reader := bufio.NewReader(conn)

	// Lê primeira mensagem (handshake)
	msgStr, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("(%s) [Broker %s] - [TCP]: erro conexão %s\n", timeStamp(), b.id, conn.RemoteAddr())
		b.removerConexao(conn)
		return
	}

	base := base{}
	err = json.Unmarshal([]byte(msgStr), &base)
	if err != nil {
		fmt.Printf("(%s) [Broker %s] - [TCP]: erro handshake\n", timeStamp(), b.id)
		return
	}

	fmt.Printf("(%s) [Broker %s] - [TCP]: handshake tipo=%s id=%s\n", timeStamp(), b.id, base.Tipo, base.ID)

	// Registra a conn no map apenas se não for um broker peer.
	if base.Tipo != "broker" {
		b.registrar(conn, base.Tipo, base.ID)
	}

	// Loop dispatcher
	for {
		msgStr, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("(%s) [Broker %s] - [TCP]: conexão perdida %s\n", timeStamp(), b.id, conn.RemoteAddr())
			b.removerConexao(conn)
			return
		}

		fmt.Printf("(%s) [Broker %s] - [TCP]: msg recebida %s\n", timeStamp(), b.id, msgStr)

		b.dispatch(conn, []byte(msgStr))
	}
}

// Comunicação com outro Broker (lado cliente)
func (b *Broker) handlePeer(address string) {
	var conn net.Conn

	for {
		c, err := net.Dial("tcp", ":"+address)
		if err == nil {
			conn = c
			break
		}
		fmt.Printf("(%s) [Broker %s] - [PEER]: tentando conectar %s\n", timeStamp(), b.id, address)
		time.Sleep(2 * time.Second)
	}

	fmt.Printf("(%s) [Broker %s] - [PEER]: conectado %s\n", timeStamp(), b.id, address)

	data, _ := json.Marshal(MensagemBroker{Tipo: "broker", ID: b.id})
	conn.Write(append(data, '\n'))

	b.registrar(conn, "broker", address)

	for msg := range b.chBroadcast {
		fmt.Printf("(%s) [Broker %s] - [PEER]: enviando %s\n", timeStamp(), b.id, msg.Reply)

		payload, _ := json.Marshal(msg)
		_, err := conn.Write(append(payload, '\n'))
		if err != nil {
			fmt.Printf("(%s) [Broker %s] - [PEER]: erro envio\n", timeStamp(), b.id)
			b.removerConexao(conn)
			return
		}
	}
}

// Registrar Conexão
func (b *Broker) registrar(conn net.Conn, tipo string, id string) {

	b.mu.Lock()
	defer b.mu.Unlock()

	fmt.Printf("(%s) [Broker %s] - [REG]: tipo=%s id=%s\n", timeStamp(), b.id, tipo, id)

	switch tipo {

	case "servico":
		if _, ok := b.servicos[conn]; !ok {
			b.servicos[conn] = id
		}

	case "recurso":
		if _, ok := b.recursos[conn]; !ok {
			b.recursos[conn] = id
		}

	case "drone":
		if _, ok := b.drones[conn]; !ok {
			b.drones[conn] = id
		}

	case "broker":
		if _, ok := b.brokers[conn]; !ok {
			b.brokers[conn] = id
		}
	}
}

// ----------- Dispatcher ----------

func (b *Broker) dispatch(conn net.Conn, data []byte) {

	base := base{}
	json.Unmarshal(data, &base)

	fmt.Printf("(%s) [Broker %s] - [DISPATCH]: tipo=%s\n", timeStamp(), b.id, base.Tipo)

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

// Aciona handlers
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

// ----------- Handlers de Mensagens ----------

func (b *Broker) handleServico(msg MensagemServico) {
	
	fmt.Printf("(%s) [Broker %s] - [SERVICO]: id=%s prioridade=%d\n", timeStamp(), b.id, msg.ID, msg.Prioridade)
	
	if msg.Prioridade > 0 {
		req := Requisicao{msg.Prioridade, msg.Timestamp, msg.ID}
		b.adicionarFila(req)
	}
}

func (b *Broker) handleRecurso(msg MensagemRecurso) {

	fmt.Printf("(%s) [Broker %s] - [RECURSO]: id=%s prioridade=%d\n", timeStamp(), b.id, msg.ID, msg.Prioridade)
	
	if msg.Prioridade > 0 {
		req := Requisicao{msg.Prioridade, msg.Timestamp, msg.ID}
		b.adicionarFila(req)
	}
}

// Gerencia a recepção e envio de mensagens de Broker com base no algoritmo Ricarti Agrawala 
func (b *Broker) handleBroker(msg MensagemBroker) {
	
	fmt.Printf("(%s) [Broker %s] - [RA]: msg=%s origem=%s\n", timeStamp(), b.id, msg.Reply, msg.ID)
	
	switch msg.Reply {

	case "REQUEST":
		fmt.Printf("(%s) [Broker %s] - [RA]: REQUEST recebido\n", timeStamp(), b.id)

		b.mu.Lock()

		var cederPassagem bool
		if b.inCS { // Se o Broker local estiver na região crítica, não cede passagem 
			cederPassagem = false
		} else if !b.requesting { // Senão se o Broker local não estiver solicitando requisição, cede passagem 
			cederPassagem = true
		} else { // Senão, envia a permissão de passagem para a região crítica a partir da comparação de prioridade entre requisições
			cederPassagem = b.temPrioridade(msg.Requisicao, b.currentReq)
		}

		if cederPassagem {
			b.mu.Unlock()
			fmt.Printf("(%s) [Broker %s] - [RA]: enviando OK\n", timeStamp(), b.id)
			b.chBroadcast <- MensagemBroker{Tipo: "broker", ID: b.id, Reply: "OK"} // Envia Ok ao Broker remoto que solicitou
		} else {
			for conn := range b.brokers {
				b.deferred = append(b.deferred, conn) // Guarda conn de Broker remoto para enviar Ok após o Broker local sair da região crítica
			}
			b.mu.Unlock()
			fmt.Printf("(%s) [Broker %s] - [RA]: OK adiado\n", timeStamp(), b.id)
		}

	case "OK":
		b.mu.Lock()
		b.okCount++ // Incrementa um Ok recebido
		total := len(b.brokers)

		fmt.Printf("(%s) [Broker %s] - [RA]: OK %d/%d\n", timeStamp(), b.id, b.okCount, total)

		if b.okCount >= total { // Se o número de Oks for igual ao de conns de Brokers, o Broker local entra na região crítica
			b.mu.Unlock()
			b.entrarCS()
		} else {
			b.mu.Unlock()
		}
	}
}

func (b *Broker) handleDrone(msg MensagemDrone) {
	
	fmt.Printf("(%s) [Broker %s] - [DRONE]: msg de %s sinal=%v\n", timeStamp(), b.id, msg.ID, msg.Sinal)
	
	if msg.Sinal {
		fmt.Printf("(%s) [Broker %s] - [DRONE]: conclusão recebida\n", timeStamp(), b.id)
		
		b.mu.Lock()
		
		if len(b.fila.Itens) > 0 {
			b.fila.Remove()
		}
		
		b.inCS = false
		
		pendentes := b.deferred
		b.deferred = nil
		
		b.mu.Unlock()
		
		for _, conn := range pendentes {
			fmt.Printf("(%s) [Broker %s] - [DRONE]: enviando OK adiado\n", timeStamp(), b.id)
			b.enviarOK(conn)
		}
		
		go b.tentarDespachar()
		
		} else {
			fmt.Printf("(%s) [Broker %s] - [DRONE]: executando %s\n", timeStamp(), b.id, msg.Acao)
		}
}

// ----------- Ricart-Agrawala ----------

// Retorna a prioridade para aquelas que tem maior nível ou menor timestamp
func (b *Broker) temPrioridade(a, outro Requisicao) bool {
	if a.Prioridade != outro.Prioridade {
		return a.Prioridade > outro.Prioridade
	}
	if a.Timestamp != outro.Timestamp {
		return a.Timestamp < outro.Timestamp
	}
	return a.Origem < outro.Origem // Caso de desempate final: a maior prioridade é por ordem alfabética entre destinatário e remetente
}

func (b *Broker) enviarOK(conn net.Conn) {
	fmt.Printf("(%s) [Broker %s] - [RA]: enviando OK direto\n", timeStamp(), b.id)
	
	resp := MensagemBroker{
		Tipo:  "broker",
		ID:    b.id,
		Reply: "OK",
	}
	data, _ := json.Marshal(resp)
	conn.Write(append(data, '\n'))
}

// ----------- CS ----------

func (b *Broker) entrarCS() {
	b.mu.Lock()
	b.inCS = true
	b.requesting = false
	b.mu.Unlock()

	fmt.Printf("\n(%s) [Broker %s] - [CS]: >> ENTRANDO NA REGIÃO CRÍTICA << \n", timeStamp(), b.id)

	droneConn := b.escolherDrone()
	if droneConn != nil {
		b.enviarParaDrone(droneConn)
	}
}

// ----------- Drone ----------
	
func (b *Broker) enviarParaDrone(conn net.Conn) {
		
		fmt.Printf("(%s) [Broker %s] - [DRONE]: enviando requisição ao drone\n", timeStamp(), b.id)
		
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
		fmt.Printf("(%s) [Broker %s] - [DRONE]: drone selecionado\n", timeStamp(), b.id)
		return conn
	}
	
	fmt.Printf("(%s) [Broker %s] - [DRONE]: nenhum drone disponível\n", timeStamp(), b.id)
	return nil
}

// ----------- Fila ----------

func (b *Broker) adicionarFila(req Requisicao) {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	fmt.Printf("(%s) [Broker %s] - [FILA]: adicionando req origem=%s prioridade=%d\n",
	timeStamp(), b.id, req.Origem, req.Prioridade)
	
	b.fila.Push(req)
	
	fmt.Printf("(%s) [Broker %s] - [FILA]: estado atual\n", timeStamp(), b.id)
	b.fila.Listar()
	
	go b.tentarDespachar()
}

// ----------- Agrawala (início) ----------

func (b *Broker) tentarDespachar() {
	
	b.mu.Lock()
	
	fmt.Printf("(%s) [Broker %s] - [RA]: tentarDespachar Reqs na Fila=%d requesting=%v inCS=%v\n",
	timeStamp(), b.id, len(b.fila.Itens), b.requesting, b.inCS)
	
	if len(b.fila.Itens) == 0 || b.requesting || b.inCS {
		b.mu.Unlock()
		return
	}
	
	b.requesting = true
	b.okCount = 0
	b.currentReq = *b.fila.Remove()
	totalPeers := len(b.brokers)
	req := b.currentReq
	
	fmt.Printf("(%s) [Broker %s] - [RA]: iniciando REQUEST %+v\n", timeStamp(), b.id, req)
	
	b.mu.Unlock()
	
	if totalPeers == 0 {
		b.entrarCS()
		return
	}
	
	msg := MensagemBroker{
		Tipo:       "broker",
		ID:         b.id,
		Reply:      "REQUEST",
		Requisicao: req,
	}
	
	b.chBroadcast <- msg
	fmt.Printf("(%s) [Broker %s] - [RA]: REQUEST enviado\n", timeStamp(), b.id)
}

// ----------- Conexões ----------

func (b *Broker) removerConexao(conn net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()

	fmt.Printf("(%s) [Broker %s] - [CONN]: removendo conexão\n", timeStamp(), b.id)

	delete(b.brokers, conn)
	delete(b.servicos, conn)
	delete(b.recursos, conn)
	delete(b.drones, conn)
}

// Retorna timeStamp atual
func timeStamp() string{
	currentTime := time.Now()

	return (fmt.Sprintf("%d-%d-%d %d:%d:%d",
		currentTime.Day(),
		currentTime.Month(),
		currentTime.Year(),
		currentTime.Hour(),
		currentTime.Minute(),
		currentTime.Second()))
}

// ----------- Main ----------

func main() {
	broker := novoBroker(os.Args[1])
	go broker.handlePeer(os.Args[3])
	broker.iniciaServidorTCP(os.Args[2])
}