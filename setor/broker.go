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
	respostasOK map[string]bool
	deferred   []string // IDs de peers cujo OK foi adiado; receberão OK após sair da CS

	// Maps de gerenciamento das conexões vivas
	brokers  map[net.Conn]MensagemBroker
	servicos map[net.Conn]MensagemServico
	recursos map[net.Conn]MensagemRecurso
	drones   map[net.Conn]MensagemDrone

	// Fila de Requisições
	fila FilaPrioridade

	// canais (dispatcher)
	chServico chan MensagemServico
	chRecurso chan MensagemRecurso
	chDrone   chan MensagemDrone
	chBroker  chan MensagemBroker
}

// ----------- Inicialização ----------

func novoBroker(id string) *Broker {
	b := &Broker{
		id: id,
		respostasOK: make(map[string]bool),

		brokers:  make(map[net.Conn]MensagemBroker),
		servicos: make(map[net.Conn]MensagemServico),
		recursos: make(map[net.Conn]MensagemRecurso),
		drones:   make(map[net.Conn]MensagemDrone),

		fila: FilaPrioridade{},

		chServico:   make(chan MensagemServico, 100),
		chRecurso:   make(chan MensagemRecurso, 100),
		chDrone:     make(chan MensagemDrone, 100),
		chBroker:    make(chan MensagemBroker, 100),
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

	// Heartbeat para outros Brokers (utiliza broadcast)
	go b.heartbeatSender(5*time.Second)

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

	// Registra conn no map apenas se não for um broker peer (peers são registrados pela porta em handlePeer).
	if base.Tipo != "broker" {
		b.registrar(conn, base)
	}

	// Loop dispatcher
	for {
		msgStr, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("(%s) [Broker %s] - [TCP]: conexão perdida %s\n", timeStamp(), b.id, conn.RemoteAddr())
			b.removerConexao(conn)
			return
		}

		//fmt.Printf("(%s) [Broker %s] - [TCP]: msg recebida %s\n", timeStamp(), b.id, msgStr)

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

	// Registro de Broker remoto ocorre aqui para obter conn de outra porta
	b.registrar(conn, base{Tipo: "broker", ID: address, Timestamp: time.Now().UnixNano()})
}

// Broadcast entre Brokers (peers)
func (b *Broker) broadcast(msg MensagemBroker) {
	b.mu.Lock()

	var toRemove []net.Conn
	data, _ := json.Marshal(msg)

	for conn := range b.brokers {
		_, err := conn.Write(append(data, '\n'))
		if err != nil {
			fmt.Printf("(%s) [BROADCAST]: erro -> marcando remoção\n", timeStamp())
			conn.Close()
			toRemove = append(toRemove, conn)
		}
	}

	b.mu.Unlock()

	// remove FORA do lock
	for _, conn := range toRemove {
		b.removerConexao(conn)
	}
}

// Registrar Conexão
func (b *Broker) registrar(conn net.Conn, msg base) {

	b.mu.Lock()
	defer b.mu.Unlock()

	fmt.Printf("(%s) [Broker %s] - [REG]: tipo=%s id=%s ts=%d\n",
		timeStamp(), b.id, msg.Tipo, msg.ID, msg.Timestamp)

	switch msg.Tipo {

	case "servico":
		b.servicos[conn] = MensagemServico{
			ID:        msg.ID,
			Timestamp: msg.Timestamp,
		}

	case "recurso":
		b.recursos[conn] = MensagemRecurso{
			ID:        msg.ID,
			Timestamp: msg.Timestamp,
		}

	case "drone":
		b.drones[conn] = MensagemDrone{
			ID:        msg.ID,
			Timestamp: msg.Timestamp,
			Estado: "FREE",
		}

		if b.inCS {
			fmt.Printf("(%s) [Broker %s] - [REG]: Drone conectado durante CS, enviando tarefa pendente\n", timeStamp(), b.id)
			b.enviarParaDrone(conn) 
		} else {
			// Caso contrário, tenta iniciar o processo de disputa normal
			go b.tentarDespachar()
		}

	case "broker":
		b.brokers[conn] = MensagemBroker{
			ID:        msg.ID,
			Timestamp: msg.Timestamp,
		}
	}
}

// ----------- Dispatcher ----------

func (b *Broker) dispatch(conn net.Conn, data []byte) {

	base := base{}
	json.Unmarshal(data, &base)

	//fmt.Printf("(%s) [Broker %s] - [DISPATCH]: tipo=%s\n", timeStamp(), b.id, base.Tipo)

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
	
	// Atualiza timestamp 
	b.mu.Lock()
	for conn, s := range b.servicos {
		if s.ID == msg.ID {
			s.Timestamp = msg.Timestamp
			b.servicos[conn] = s
			break
		}
	}
	b.mu.Unlock()

	if msg.Prioridade > 0 {
		req := Requisicao{msg.Prioridade, msg.Timestamp, msg.ID}
		b.adicionarFila(req)
	}
}

func (b *Broker) handleRecurso(msg MensagemRecurso) {

	fmt.Printf("(%s) [Broker %s] - [RECURSO]: id=%s prioridade=%d\n", timeStamp(), b.id, msg.ID, msg.Prioridade)
	
	// Atualiza timestamp
	b.mu.Lock()
	for conn, r := range b.recursos {
		if r.ID == msg.ID {
			r.Timestamp = msg.Timestamp
			b.recursos[conn] = r
			break
		}
	}
	b.mu.Unlock()

	if msg.Prioridade > 0 {

		req := Requisicao{msg.Prioridade, msg.Timestamp, msg.ID}
		b.adicionarFila(req)
	}
}

// Gerencia a recepção e o envio de mensagens de Broker com base no algoritmo Ricarti Agrawala 
func (b *Broker) handleBroker(msg MensagemBroker) {
	
	if msg.Reply != "HEARTBEAT" { // Só exibe mensagem se não for heartbeat
		fmt.Printf("(%s) [Broker %s] - [BROKER]: msg=%s origem=%s\n", timeStamp(), b.id, msg.Reply, msg.ID)
	}

	switch msg.Reply {

	// Apenas mensagem de heartbeat para indicar que a conexão está viva
	case "HEARTBEAT":
		b.mu.Lock()
		for conn, br := range b.brokers {
			if br.ID == msg.ID {
				br.Timestamp = msg.Timestamp
				b.brokers[conn] = br
				break
			}
		}
		b.mu.Unlock()

	case "REQUEST":
        b.mu.Lock()
        var cederPassagem bool
        if b.inCS {
            cederPassagem = false
        } else if !b.requesting {
            cederPassagem = true
        } else {
            cederPassagem = b.temPrioridade(msg.Requisicao, b.currentReq)
        }

        if cederPassagem {
            // Se eu cedi para ele e estou disputando, o OK que ele me deu antes não vale mais
            if b.requesting {
                delete(b.respostasOK, msg.ID)
            }
            b.mu.Unlock()
            b.enviarOK(msg.ID)
        } else {
            b.deferred = append(b.deferred, msg.ID)
            b.mu.Unlock()
        }

    case "OK":
        b.mu.Lock()
        if !b.requesting {
            b.mu.Unlock()
            return
        }

        b.respostasOK[msg.ID] = true
        total := len(b.brokers)
        recebidos := len(b.respostasOK)

        fmt.Printf("(%s) [Broker %s] - [RA]: OK %d/%d\n", timeStamp(), b.id, recebidos, total)

        if recebidos >= total {
            // Transição ATÔMICA: define estados antes de liberar o lock
            b.inCS = true
            b.requesting = false
            // Limpa o mapa para a próxima disputa
            for k := range b.respostasOK { delete(b.respostasOK, k) }
            
            b.mu.Unlock()
            b.entrarCS()
        } else {
            b.mu.Unlock()
        }
	}
}

func (b *Broker) handleDrone(msg MensagemDrone) {
	// 1. Atualização de Logs e Timestamp para o Heartbeat Monitor
	if msg.Acao != "heartbeat" { // Só exibe mensagem se não for heartbeat
	fmt.Printf("(%s) [Broker %s] - [DRONE]: msg de %s | Ação: %s | Sinal: %v\n",
		timeStamp(), b.id, msg.ID, msg.Acao, msg.Sinal)
	}

	// Atualiza timestamp do drone
	b.mu.Lock()
	for conn, d := range b.drones {
		if d.ID == msg.ID {
			d.Timestamp = msg.Timestamp
			d.Estado = msg.Estado
			b.drones[conn] = d
			break
		}
	}
	b.mu.Unlock()

	// 2. Tratamento de Conclusão de Tarefa (Sinal de saída da CS)
	if msg.Sinal {
		fmt.Printf("(%s) [Broker %s] - [CS]: Tarefa concluída pelo drone. Saindo da Região Crítica.\n",
			timeStamp(), b.id)

		b.mu.Lock()
		
		// Reset do estado de execução
		b.inCS = false
		b.currentReq = Requisicao{} // Limpa a requisição atual que foi processada
		b.requesting = false


		// 3. Gerenciamento de OKs Adiados (Ricart-Agrawala)
		// Captura os peers que ficaram esperando enquanto este broker usava o drone
		pendentes := b.deferred
		b.deferred = nil 
		
		b.mu.Unlock()

		// Exibe fila 
		b.fila.Listar()

		// Envia as permissões (OK) para os outros brokers que solicitaram a CS
		for _, id := range pendentes {
			b.enviarOK(id)
		}

		// 4. Verificação de Nova Demanda
		// Agora que liberamos a CS, verificamos se há algo novo na fila para disputar
		go b.tentarDespachar()

	} else {
		// Apenas log de acompanhamento do drone
		fmt.Printf("(%s) [Broker %s] - [DRONE]: Status recebido - %s (Estado: %s)\n",
			timeStamp(), b.id, msg.Acao, msg.Estado)
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

func (b *Broker) enviarOK(ID string) {
	fmt.Printf("(%s) [Broker %s] - [RA]: enviando OK\n", timeStamp(), b.id)
	
	var c net.Conn
	b.mu.Lock()
	for conn, b := range b.brokers {
		if b.ID == ID {
			c = conn
		}
	}
	
	if c == nil {
		b.mu.Unlock()
		return // Não encontrou conn do Broker remetente
	}
	
	b.fila.Aging() // Envelhece a fila do Broker local para cada rodada que v
	b.mu.Unlock()

	resp := MensagemBroker{
		Tipo:  "broker",
		ID:    b.id,
		Reply: "OK",
	}
	data, _ := json.Marshal(resp)
	c.Write(append(data, '\n'))
}

// ----------- CS ----------

func (b *Broker) entrarCS() {
    b.mu.Lock()
    b.inCS = true
    // Mantemos a b.currentReq ativa no estado do Broker até o drone terminar
    b.mu.Unlock()

    fmt.Printf("\n(%s) [Broker %s] - [CS]: >> ENTRANDO NA REGIÃO CRÍTICA << \n", timeStamp(), b.id)

    droneConn := b.escolherDrone()
    if droneConn != nil {
        b.enviarParaDrone(droneConn)
    } else {
        fmt.Printf("(%s) [Broker %s] - [CS]: Aguardando conexão de drone para processar: %+v\n", timeStamp(), b.id, b.currentReq)
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

	// Verificação de saída: se não houver itens ou se já estivermos ocupados
	if len(b.fila.Itens) == 0 || b.requesting || b.inCS {
		b.mu.Unlock()
		return
	}

	// Inicia a disputa
	b.requesting = true
	// Limpa registros de OKs antigos para esta nova rodada
	for k := range b.respostasOK {
		delete(b.respostasOK, k)
	}
	
	b.currentReq = *b.fila.Remove()
	totalPeers := len(b.brokers)
	req := b.currentReq

	fmt.Printf("(%s) [Broker %s] - [RA]: iniciando REQUEST %+v\n", timeStamp(), b.id, req)
	
	// IMPORTANTE: Soltamos o lock antes de iniciar ações externas (Broadcast ou CS)
	// para evitar que os handlers de resposta fiquem travados esperando o mutex.
	b.mu.Unlock()

	// Se não há outros brokers, entra direto na região crítica
	if totalPeers == 0 {
		b.entrarCS()
		return
	}

	// Caso contrário, faz o broadcast do REQUEST
	msg := MensagemBroker{
		Tipo:       "broker",
		ID:         b.id,
		Reply:      "REQUEST",
		Requisicao: req,
	}

	b.broadcast(msg)
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

// HeartbeatSender: Envia heartbeat a todos os Brokers pra sinalizar que este Broker está conectado
func (b *Broker) heartbeatSender(timeout time.Duration) {
	ticker := time.NewTicker(timeout / 2)
	defer ticker.Stop()

	fmt.Println("HEARTBEATSENDER INICIADO")

	for range ticker.C {

		b.mu.Lock()
		if len(b.brokers) == 0 {
			b.mu.Unlock()
			continue
		}
		b.mu.Unlock()

		msg := MensagemBroker{
			Tipo:      "broker",
			ID:        b.id,
			Reply:     "HEARTBEAT",
			Timestamp: time.Now().UnixNano(),
		}

		b.broadcast(msg)
	}
}

// HeartbeatMonitor: monitora sinais de conexão ativa de todos os dispositivos
func (b *Broker) heartbeatMonitor(timeout time.Duration) {

	ticker := time.NewTicker(timeout)
	defer ticker.Stop()

	fmt.Println("HEARTBEAT MONITOR INICIADO")

	for range ticker.C {

		now := time.Now().UnixNano()

		var mortosDrones []net.Conn
		var mortosBrokers []net.Conn
		var mortosServicos []net.Conn
		var mortosRecursos []net.Conn

		b.mu.Lock()

		// -------- DRONES --------
		for conn, msg := range b.drones {
			if msg.Timestamp == 0 {
				continue // ainda não recebeu heartbeat
			}

			if now-msg.Timestamp > int64(timeout)*2 {
				fmt.Printf("(%s) [HB]: DRONE %s morto\n",
					timeStamp(), msg.ID)

				mortosDrones = append(mortosDrones, conn)
			}
		}

		// -------- BROKERS --------
		for conn, msg := range b.brokers {
			if msg.Timestamp == 0 {
				continue
			}

			if now-msg.Timestamp > int64(timeout)*2 {
				fmt.Printf("(%s) [HB]: BROKER %s morto\n",
					timeStamp(), msg.ID)

				mortosBrokers = append(mortosBrokers, conn)
			}
		}
		
		// -------- SERVIÇOS --------
		for conn, msg := range b.servicos {
			if msg.Timestamp == 0 {
				continue
			}

			if now-msg.Timestamp > int64(timeout)*2 {
				fmt.Printf("(%s) [HB]: SERVICO %s morto\n",
					timeStamp(), msg.ID)

				mortosServicos = append(mortosServicos, conn)
			}
		}

		// -------- RECURSOS --------
		for conn, msg := range b.recursos {
			if msg.Timestamp == 0 {
				continue
			}

			if now-msg.Timestamp > int64(timeout)*2 {
				fmt.Printf("(%s) [HB]: RECURSO %s morto\n",
					timeStamp(), msg.ID)

				mortosRecursos = append(mortosRecursos, conn)
			}
		}

		b.mu.Unlock()

		// -------- REMOÇÃO FORA DO LOCK --------
		for _, conn := range mortosDrones {
			b.removerConexao(conn)
		}

		for _, conn := range mortosBrokers {
			b.removerConexao(conn)
		}

		for _, conn := range mortosServicos {
			b.removerConexao(conn)
		}

		for _, conn := range mortosRecursos {
			b.removerConexao(conn)
		}
	}
}

// ----------- Main ----------

func main() {
	broker := novoBroker(os.Args[1]) // ID do Broker é a própria porta em que está rodando
	
	for i := 2; i < len(os.Args); i++ {
		go broker.handlePeer(os.Args[i])
	}
	go broker.heartbeatMonitor(5 * time.Second)
	broker.iniciaServidorTCP(os.Args[1])
}