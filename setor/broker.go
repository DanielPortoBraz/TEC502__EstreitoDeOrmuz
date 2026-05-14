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
	"crypto/tls"
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

	relogioLocal int64 // Relógio Lógico (Lamport): usado em eventos significativos no uso de recursos compartilhados

	// --- Estado Ricart-Agrawala (adaptado) ---
	requesting bool       // true enquanto aguarda OKs dos peers
	attending  bool       // true se o broker já está sendo atendido por um drone
	inCS       bool       // true enquanto usa o drone (região crítica)
	currentReq Requisicao // requisição que este broker está disputando no momento
	respostasOK map[string]bool // OKs recebidos e utilizados para entrar na CS
	deferred   map[string]struct{} // IDs de peers cujo OK foi adiado; receberão OK após sair da CS. Garante que seja um OK por ID somente

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
		deferred: make(map[string]struct{}),
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

// -------- Utils------------

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// ----------- Servidor TCP ----------

func (b *Broker) iniciaServidorTCP(porta string) {
	cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
	if err != nil {
		panic(err)
	}

	config := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	listener, err := tls.Listen("tcp", ":"+porta, config)

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
		config := &tls.Config{
			InsecureSkipVerify: true,
		}

		c, err := tls.Dial("tcp", ":"+address, config)
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
			Estado: "BUSY",
		}

		if b.inCS{

			if !b.attending {
				fmt.Printf("(%s) [Broker %s] - [REG]: Drone conectado durante CS, enviando tarefa pendente\n", timeStamp(), b.id)

				b.mu.Lock()
				b.attending = true
				b.mu.Unlock()
				
				b.enviarParaDrone(conn) 
			} else {
				// Eu já estou sendo atendido e um drone ficou livre.
				b.liberarAdiamentos()
			}

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

	// Evento interno (chegou tarefa), incrementa relógio
	b.relogioLocal++
	carimboDaTarefa := b.relogioLocal

	b.mu.Unlock()
	
	if msg.Prioridade > 0 {
		req := Requisicao{msg.Prioridade, carimboDaTarefa, msg.ID}
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
	
	// Evento interno (chegou tarefa), incrementa relógio
	b.relogioLocal++
	carimboDaTarefa := b.relogioLocal
	b.mu.Unlock()
	
	if msg.Prioridade > 0 {
		req := Requisicao{msg.Prioridade, carimboDaTarefa, msg.ID}
		b.adicionarFila(req)
	}
}

// Gerencia a recepção e o envio de mensagens de Broker com base no algoritmo Ricarti Agrawala 
func (b *Broker) handleBroker(msg MensagemBroker) {
	
	if msg.Reply != "HEARTBEAT" { // Só exibe mensagem se não for heartbeat
		fmt.Printf("(%s) [Broker %s] - [BROKER]: msg=%s origem=%s\n", timeStamp(), b.id, msg.Reply, msg.ID)
	}
	
	
	b.mu.Lock()
	// Atualiza relógio lógico para o valor mais alto (local ou remoto)
	b.relogioLocal = max(b.relogioLocal, msg.Relogio) + 1

	for conn, br := range b.brokers {
		if br.ID == msg.ID {
			br.Timestamp = msg.Timestamp
			b.brokers[conn] = br
			break
		}
	}
	b.mu.Unlock()

	switch msg.Reply {

	case "REQUEST":

		fmt.Println("RECEBI UM REQUEST ===============")


        b.mu.Lock()
        var cederPassagem bool
        if b.inCS { // Se estou na CS não envio o OK 
			cederPassagem = false

        } else if !b.requesting {
            cederPassagem = true

        } else {

			if b.respostasOK[msg.ID] {
                // Se ele já me deu OK, eu não cedo passagem de jeito nenhum!
                cederPassagem = false
			} else {			 
            	cederPassagem = b.temPrioridade(msg.Requisicao, b.currentReq)
			}
        }

        if cederPassagem {
			b.fila.Aging() // Envelhece a minha fila toda vez q eu dou passagem

			delete(b.deferred, msg.ID) // Ao enviar OK, retiro qualquer OK adiado para aquele Broker
            b.mu.Unlock()
            b.enviarOK(msg.ID)
			
        } else {
            b.deferred[msg.ID] = struct{}{}
            b.mu.Unlock()
        }

    case "OK":
        b.mu.Lock()

        if !b.requesting || b.inCS {
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

	// Tratamento de Rejeição (Conflito de Corrida na Rede)
	if msg.Acao == "rejeitado" {
		fmt.Printf("(%s) [Broker %s] - [DRONE]: Requisição rejeitada por conflito. Retentando...\n", timeStamp(), b.id)
		
		b.mu.Lock()
		b.attending = false // Retira a flag: não conseguimos o drone
		b.mu.Unlock()

		// Tenta pegar outro drone imediatamente
		conn := b.escolherDrone()
		if conn != nil {
			b.mu.Lock()
			b.attending = true
			b.mu.Unlock()	

			b.enviarParaDrone(conn)
		}
		return
	}

	// 2. Tratamento de Conclusão de Tarefa (Sinal de saída da CS)
	if msg.Sinal {
		fmt.Printf("(%s) [Broker %s] - [CS]: Tarefa concluída pelo drone. Saindo da Região Crítica.\n",
			timeStamp(), b.id)

		b.mu.Lock()
		
		// Reset do estado de execução
		b.inCS = false
		b.attending = false
		b.currentReq = Requisicao{} // Limpa a requisição atual que foi processada
		b.requesting = false 

		b.mu.Unlock()

		// 3. Gerenciamento de OKs Adiados (Ricart-Agrawala)
		// Captura os peers que ficaram esperando enquanto este broker usava o drone
		b.liberarAdiamentos()

		// Exibe fila 
		b.fila.Listar()

		// 4. Verificação de Nova Demanda
		// Agora que liberamos a CS, verificamos se há algo novo na fila para disputar
		go b.tentarDespachar()

	} else if msg.Estado == "FREE" && msg.Acao != "andamento" { // Se o drone está livre, tentamos disparar a fila imediatamente

		if b.inCS { // Se eu estiver esperando CS, escolho este drone livre caso não tenha sido atendido por outro

			if !b.attending {
				fmt.Printf("... Retomando tarefa ...\n")
				conn := b.escolherDrone()

				if conn != nil {
					b.mu.Lock()
					b.attending = true
					b.mu.Unlock()	

					b.enviarParaDrone(conn)
				}
			} else {
				// Eu já estou sendo atendido e um drone ficou livre.
				b.liberarAdiamentos()
			}
		} else {
			go b.tentarDespachar()
		}
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
	fmt.Printf("(%s) [Broker %s] - [RA]: enviando OK para %s\n", timeStamp(), b.id, ID)
	
	var c net.Conn
	b.mu.Lock()
	for conn, br := range b.brokers {
		if br.ID == ID {
			c = conn
		}
	}
	
	if c == nil {
		b.mu.Unlock()
		return // Não encontrou conn do Broker remetente
	}

	b.relogioLocal++ 
	relogioAtual := b.relogioLocal

	b.mu.Unlock()

	resp := MensagemBroker{
		Tipo:  "broker",
		ID:    b.id,
		Reply: "OK",
		Relogio: relogioAtual,
		Timestamp: time.Now().UnixNano(),
	}
	data, _ := json.Marshal(resp)
	c.Write(append(data, '\n'))
}

func (b *Broker) liberarAdiamentos() {
	b.mu.Lock()
	pendentes := make(map[string]struct{})
	for k := range b.deferred {
		pendentes[k] = struct{}{}
		delete(b.deferred, k)
	}
	b.mu.Unlock()

	for id := range pendentes {
		b.enviarOK(id) // Dispara os OKs retidos
	}
}

// ----------- CS ----------

func (b *Broker) entrarCS() {
    b.mu.Lock()
    b.inCS = true
    // Mantemos a b.currentReq ativa no estado do Broker até o drone terminar
    b.mu.Unlock()
	
	fmt.Printf(
		"\n(%s) [Broker %s] - [CS]: >> ENTRANDO NA REGIÃO CRÍTICA (DRONE de IP) << \n",
		timeStamp(),
		b.id,
	)
    droneConn := b.escolherDrone()

	if droneConn != nil {
		b.mu.Lock()
		b.attending = true
		b.mu.Unlock()

		b.enviarParaDrone(droneConn)

		if b.quantidadeDronesLivres() > 0 {
			b.liberarAdiamentos()
		}
		} else {
		fmt.Printf(
			"(%s) [Broker %s] - [CS]: Aguardando conexão de drone para processar: %+v\n",
			timeStamp(),
			b.id,
			b.currentReq,
		)
	}
}

// ----------- Drone ----------

func (b *Broker) quantidadeDronesLivres() int{
	b.mu.Lock()

	dronesLivres := 0
	for _, d := range b.drones {
		if d.Estado == "FREE" {
			dronesLivres++
		}
	}

	b.mu.Unlock()

	return dronesLivres
}
	
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

	for conn, d := range b.drones {
		if d.Estado == "FREE" {
			fmt.Printf("(%s) [Broker %s] - [DRONE]: drone selecionado\n", timeStamp(), b.id)
			d.Estado = "BUSY"
			b.drones[conn] = d
			return conn
		}
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
	
	// 1. Verificação de drones livres
	dronesLivres := b.quantidadeDronesLivres()
	
	b.mu.Lock()

	// 2. Condição de saída: fila vazia ou o broker já está ocupado com algo
	// Adicionado check de b.requesting para evitar re-requests desnecessários
	if len(b.fila.Itens) == 0 || b.inCS || b.requesting || b.attending {
		b.mu.Unlock()
		return
	}

	// 3. Lógica de Bypass (Drones para todos)
	if dronesLivres >= (len(b.brokers) + 1) {
		b.currentReq = *b.fila.Remove()
		fmt.Printf("(%s) [Broker %s] - [BYPASS]: Drones suficientes (%d). Despacho direto.\n", 
			timeStamp(), b.id, dronesLivres)
		
		// IMPORTANTE: Definir inCS como true para evitar novos despachos enquanto o drone trabalha
		b.inCS = true 
		b.mu.Unlock()

		conn := b.escolherDrone()
		if conn != nil {
			b.mu.Lock()
			b.attending = true
			b.mu.Unlock()

			b.enviarParaDrone(conn)
		} else {
			// Se falhou em pegar a conn, reseta estado para tentar de novo depois
			b.mu.Lock()
			b.inCS = false
			b.mu.Unlock()
		}
		return
	}

	// 4. Início do Ricart-Agrawala (Disputa por recurso escasso)
	b.requesting = true
	
	totalPeers := len(b.brokers)
	b.currentReq = *b.fila.Remove()
	req := b.currentReq 

	fmt.Printf("(%s) [Broker %s] - [RA]: iniciando REQUEST %+v\n", timeStamp(), b.id, req)
	
	b.mu.Unlock() // Libera para permitir a chegada de OKs enquanto faz o broadcast

	if totalPeers == 0 {
		b.entrarCS()
		return
	}

	b.mu.Lock()
	b.relogioLocal++
	relogioAtual := b.relogioLocal
	b.mu.Unlock()

	msg := MensagemBroker{
		Tipo:       "broker",
		ID:         b.id,
		Reply:      "REQUEST",
		Relogio:    relogioAtual,
		Timestamp:  time.Now().UnixNano(),
		Requisicao: req,
	}

	b.broadcast(msg)
	fmt.Printf("(%s) [Broker %s] - [RA]: REQUEST enviado\n", timeStamp(), b.id)
}

// ----------- Conexões ----------

func (b *Broker) removerConexao(conn net.Conn) {
	b.mu.Lock()

	fmt.Printf("(%s) [Broker %s] - [CONN]: removendo conexão\n",
		timeStamp(), b.id)

	// Verifica se era broker ANTES de remover
	br, eraBroker := b.brokers[conn]

	// Remove dos maps
	delete(b.brokers, conn)
	delete(b.servicos, conn)
	delete(b.recursos, conn)
	delete(b.drones, conn)

	b.mu.Unlock()

	// Reconexão apenas para brokers
	if eraBroker {

	b.mu.Lock()

	// Remove OK pendente daquele broker morto
	delete(b.respostasOK, br.ID)

	// Remove adiamento também
	delete(b.deferred, br.ID)

	// Se eu estava esperando OKs,
	// reavalia condição de entrada
	if b.requesting && !b.inCS {

		total := len(b.brokers)
		recebidos := len(b.respostasOK)

		fmt.Printf("(%s) [Broker %s] - [RA]: reavaliando quorum %d/%d\n",
			timeStamp(), b.id, recebidos, total)

		if recebidos >= total {

			b.inCS = true
			b.requesting = false

			for k := range b.respostasOK {
				delete(b.respostasOK, k)
			}

			b.mu.Unlock()

			go b.entrarCS()
		} else {
			b.mu.Unlock()
		}

	} else {
		b.mu.Unlock()
	}

	// Tenta handshake com o Broker da outra porta, mas evita que ocorrar reconexão bloqueante
	go func() {
		time.Sleep(3 * time.Second)
		b.handlePeer(br.ID)
	}()
	}
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