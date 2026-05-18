package main

// tester.go — cliente de teste para o sistema de Brokers/Drones
//
// Uso:
//   go run tester.go mensagens.go fila.go <broker-addr> [broker-addr2 ...]
//
// Exemplos de comandos interativos:
//   ping  <broker-addr>                     — mede latência RTT
//   req   <broker-addr> <prioridade> <n>    — envia N requisições com a prioridade dada
//   watch                                   — exibe mensagens RA (REQUEST/OK) em tempo real (sempre ativo)
//   quit

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func sqrt(x float64) float64 {
	z := x
	for i := 0; i < 10; i++ {
		z -= (z*z - x) / (2 * z)
	}
	return z
}

// -------- Tester --------

type Tester struct {
	mu sync.Mutex
	id string

	// map addr → conn estabelecida com aquele broker
	conns map[string]net.Conn

	// Relógio lógico próprio (Lamport), para timestamps das requisições
	relogio int64

	// Canal onde chegam todas as mensagens recebidas de brokers
	incoming chan mensagemRecebida

	// Modo de OK: true = responde automaticamente, false = aguarda comando manual
	autoOK bool

	pendentes map[string]string

	// Canal exclusivo para respostas PONG (evita disputa com processar())
	pong chan mensagemRecebida
}

type mensagemRecebida struct {
	addr string
	msg  MensagemBroker
}

// -------- Inicialização --------

func novoTester(addrs []string) *Tester {
	t := &Tester{
		id:        fmt.Sprintf("tester-%d", time.Now().UnixNano()%10000),
		conns:     make(map[string]net.Conn),
		incoming:  make(chan mensagemRecebida, 256),
		autoOK:    true,
		pendentes: make(map[string]string),
		pong:      make(chan mensagemRecebida, 1),
	}

	for _, addr := range addrs {
		t.conectar(addr)
	}

	return t
}

// -------- Conexão --------

func tlsDial(addr string) (net.Conn, error) {
	return tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
}

// conectar estabelece a conexão com um broker e a mantém em background.
// O tester se anuncia como tipo "broker" para entrar no map de peers e
// receber os broadcasts do RA. Em troca, responde OK a todo REQUEST recebido
// (comportamento de broker passivo — sempre cede passagem).
func (t *Tester) conectar(addr string) {
	conn, err := tlsDial(addr)
	if err != nil {
		fmt.Printf("[TESTER] erro ao conectar %s: %v\n", addr, err)
		return
	}

	// Handshake: apresenta-se como "broker" para entrar no map de peers
	handshake := MensagemBroker{
		Tipo:      "broker",
		ID:        t.id,
		Timestamp: time.Now().UnixNano(),
	}
	data, _ := json.Marshal(handshake)
	conn.Write(append(data, '\n'))

	t.mu.Lock()
	t.conns[addr] = conn
	t.mu.Unlock()

	fmt.Printf("[TESTER] conectado a %s\n", addr)

	go t.receberMensagens(addr, conn)
	go t.heartbeat(addr, conn, 5*time.Second)
}

func (t *Tester) heartbeat(addr string, conn net.Conn, interval time.Duration) {
	ticker := time.NewTicker(interval / 2)
	defer ticker.Stop()

	for range ticker.C {

		msg := MensagemBroker{
			Tipo:      "broker",
			ID:        t.id,
			Reply:     "HEARTBEAT",
			Timestamp: time.Now().UnixNano(),
		}

		data, _ := json.Marshal(msg)

		_, err := conn.Write(append(data, '\n'))
		if err != nil {
			fmt.Printf("[HEARTBEAT] conexão perdida com %s\n", addr)

			t.mu.Lock()
			delete(t.conns, addr)
			t.mu.Unlock()

			conn.Close()
			return
		}
	}
}

// receberMensagens lê continuamente mensagens do broker e as encaminha ao canal central.
func (t *Tester) receberMensagens(addr string, conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("[TESTER] conexão perdida com %s\n", addr)
			
			return
		}

		var msg MensagemBroker
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			continue
		}

		t.incoming <- mensagemRecebida{addr: addr, msg: msg}
	}
}

// -------- Loop de eventos --------

// processar lida com mensagens recebidas dos brokers.
// Roda em goroutine separada para não bloquear a leitura do stdin.
func (t *Tester) processar() {
	for recv := range t.incoming {
		msg := recv.msg
		addr := recv.addr

		switch msg.Reply {

		case "REQUEST":
			fmt.Printf("\n[RA] REQUEST  broker=%-8s  prioridade=%-3d  ts=%-20d  origem=%s  (via %s)\n",
				msg.ID, msg.Requisicao.Prioridade, msg.Requisicao.Timestamp, msg.Requisicao.Origem, addr)

			t.mu.Lock()
			auto := t.autoOK
			if !auto {
				t.pendentes[msg.ID] = addr
				fmt.Printf("[RA] REQUEST pendente — use: ok %s\n", msg.ID)
			}
			t.mu.Unlock()

			if auto {
				t.enviarOK(addr, msg.ID)
			}

		case "OK":
			fmt.Printf("[RA] OK       broker=%-8s  relogio=%-6d  (via %s)\n",
				msg.ID, msg.Relogio, addr)

		case "PONG":
			// Entrega ao canal dedicado; ping() estará esperando lá
			t.pong <- recv

		case "HEARTBEAT":
			// Ignora heartbeats de outros brokers

		default:
			fmt.Printf("[TESTER] msg desconhecida: %+v\n", msg)
		}
	}
}

// -------- Comandos --------

// ping mede a latência RTT com um broker específico.
func (t *Tester) ping(addr string) {
	t.mu.Lock()
	conn, ok := t.conns[addr]
	t.mu.Unlock()

	if !ok {
		fmt.Printf("[TESTER] sem conexão com %s\n", addr)
		return
	}

	const count = 3

	var (
		rtts     []float64
		enviados int
		recebidos int
	)

	inicio := time.Now()

	for i := 0; i < count; i++ {
		ping := MensagemBroker{
			Tipo:      "broker",
			ID:        t.id,
			Reply:     "PING",
			Timestamp: time.Now().UnixNano(),
		}

		data, _ := json.Marshal(ping)

		t0 := time.Now()
		enviados++

		_, err := conn.Write(append(data, '\n'))
		if err != nil {
			fmt.Printf("[PING] erro ao enviar ping: %v\n", err)
			continue
		}

		select {
		case <-t.pong:
			rtt := float64(time.Since(t0).Microseconds()) / 1000.0
			rtts = append(rtts, rtt)
			recebidos++

			fmt.Printf("[PING] %s: seq=%d time=%.3f ms\n",
				addr, i+1, rtt)

		case <-time.After(3 * time.Second):
			fmt.Printf("[PING] %s: seq=%d timeout\n", addr, i+1)
		}

		time.Sleep(1 * time.Second)
	}

	total := time.Since(inicio).Milliseconds()
	packetLoss := float64(enviados-recebidos) / float64(enviados) * 100

	fmt.Printf("\n--- %s ping statistics ---\n", addr)
	fmt.Printf("%d packets transmitted, %d received, %.0f%% packet loss, time %dms\n",
		enviados, recebidos, packetLoss, total)

	if recebidos > 0 {
		min := rtts[0]
		max := rtts[0]
		sum := 0.0

		for _, rtt := range rtts {
			if rtt < min {
				min = rtt
			}
			if rtt > max {
				max = rtt
			}
			sum += rtt
		}

		avg := sum / float64(len(rtts))

		// mdev (desvio médio)
		var variance float64
		for _, rtt := range rtts {
			diff := rtt - avg
			variance += diff * diff
		}

		mdev := variance / float64(len(rtts))
		mdev = sqrt(mdev)

		fmt.Printf("rtt min/avg/max/mdev = %.3f/%.3f/%.3f/%.3f ms\n",
			min, avg, max, mdev)
	}
}

// enviarRequisicoes envia n requisições com a prioridade dada ao broker no addr.
// Cada requisição chega como MensagemRecurso (prioridade > 0 entra na fila do broker).
func (t *Tester) enviarRequisicoes(addr string, prioridade int64, n int) {
	t.mu.Lock()
	conn, ok := t.conns[addr]
	t.mu.Unlock()

	if !ok {
		fmt.Printf("[TESTER] sem conexão com %s\n", addr)
		return
	}

	for i := 0; i < n; i++ {
		t.mu.Lock()
		t.relogio++
		ts := t.relogio
		t.mu.Unlock()

		msg := MensagemRecurso{
			Tipo:       "recurso",
			ID:         fmt.Sprintf("%s-req%d", t.id, ts),
			Prioridade: prioridade,
			Timestamp:  time.Now().UnixNano(),
		}
		data, _ := json.Marshal(msg)
		_, err := conn.Write(append(data, '\n'))
		if err != nil {
			fmt.Printf("[TESTER] erro ao enviar req %d: %v\n", i+1, err)
			return
		}
		fmt.Printf("[REQ] enviada #%d  prioridade=%d  broker=%s\n", i+1, prioridade, addr)
		time.Sleep(50 * time.Millisecond) // pequeno intervalo para não saturar o canal
	}
}

// enviarOK envia OK ao broker de ID brokerID através da conn em addr.
// Necessário para não bloquear o quórum do RA.
func (t *Tester) enviarOK(addr string, brokerID string) {
	t.mu.Lock()
	conn, ok := t.conns[addr]
	t.mu.Unlock()

	if !ok {
		return
	}

	ok_msg := MensagemBroker{
		Tipo:      "broker",
		ID:        t.id,
		Reply:     "OK",
		Timestamp: time.Now().UnixNano(),
	}
	data, _ := json.Marshal(ok_msg)
	conn.Write(append(data, '\n'))

	fmt.Printf("[RA] OK enviado para broker=%s (cede passagem)\n", brokerID)
}

// -------- CLI --------

func (t *Tester) cli() {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("╔══════════════════════════════════════════╗")
	fmt.Println("║           TESTER — Sistema Brokers       ║")
	fmt.Println("╠══════════════════════════════════════════╣")
	fmt.Println("║  ping    <addr>                          ║")
	fmt.Println("║  req     <addr> <prioridade> <n>         ║")
	fmt.Println("║  autoOK  <on|off>   (padrão: on)         ║")
	fmt.Println("║  ok      <brokerID>                      ║")
	fmt.Println("║  quit                                    ║")
	fmt.Println("╚══════════════════════════════════════════╝")
	fmt.Print("> ")

	for scanner.Scan() {
		linha := strings.TrimSpace(scanner.Text())
		partes := strings.Fields(linha)
		if len(partes) == 0 {
			fmt.Print("> ")
			continue
		}

		switch partes[0] {

		case "ping":
			if len(partes) < 2 {
				fmt.Println("uso: ping <addr>")
				break
			}
			go t.ping(partes[1])

		case "req":
			if len(partes) < 4 {
				fmt.Println("uso: req <addr> <prioridade> <n>")
				break
			}
			addr := partes[1]
			prio, err1 := strconv.ParseInt(partes[2], 10, 64)
			n, err2 := strconv.Atoi(partes[3])
			if err1 != nil || err2 != nil || n <= 0 {
				fmt.Println("prioridade e n devem ser inteiros positivos")
				break
			}
			go t.enviarRequisicoes(addr, prio, n)

		case "autoOK":
			if len(partes) < 2 {
				fmt.Println("uso: autoOK <on|off>")
				break
			}
			t.mu.Lock()
			switch partes[1] {
			case "on":
				t.autoOK = true
				fmt.Println("[TESTER] modo automático ativado")
			case "off":
				t.autoOK = false
				fmt.Println("[TESTER] modo manual ativado — use: ok <brokerID>")
			default:
				fmt.Println("uso: autoOK <on|off>")
			}
			t.mu.Unlock()

		case "ok":
			if len(partes) < 2 {
				fmt.Println("uso: ok <brokerID>")
				break
			}
			brokerID := partes[1]
			t.mu.Lock()
			addr, existe := t.pendentes[brokerID]
			if existe {
				delete(t.pendentes, brokerID)
			}
			t.mu.Unlock()

			if !existe {
				fmt.Printf("[TESTER] nenhum REQUEST pendente de %s\n", brokerID)
				break
			}
			go t.enviarOK(addr, brokerID)

		case "quit":
			fmt.Println("encerrando.")
			os.Exit(0)

		default:
			fmt.Printf("comando desconhecido: %q\n", partes[0])
		}

		fmt.Print("> ")
	}
}

// -------- Main --------

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "uso: go run tester.go mensagens.go fila.go <addr> [addr2 ...]\n")
		os.Exit(1)
	}

	addrs := os.Args[1:]
	tester := novoTester(addrs)

	// Processa mensagens recebidas em background
	go tester.processar()

	// CLI bloqueante
	tester.cli()
}